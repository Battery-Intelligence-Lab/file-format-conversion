function mat_to_parquet(options)
% A Matlab function to convert time series data from mat files to a Parquet file.
% 
% It assumes MAT files are stored as 'Campaigns', where a directory contains
% multiple campaigns, each of which contains one subdirectory per experiment,
% each of which contains one or more MAT files that belong to the same experiment.
% 
% It will write out a Parquet file for each experiment in the directory of its
% campaign. It assumes if an experiment consists of multiple data files, their
% names are in alphabetical order.
% 
% So if the input files are
%     START_DIRECTORY
%     └── Campaign1
%         ├── Experiment1
%         │   ├── Data1.mat
%         │   └── Data2.mat
%         └── Experiment2
%             └── Data1.mat
% The output will be at
%     START_DIRECTORY
%     └── Campaign1
%         ├── Experiment1.parquet
%         └── Experiment2.parquet

% The input is a structure containing the following optional fields:
% - DirectoryPattern: regular expression pattern of the Campaign directories.
% - SubdirectoryPattern: regular expression pattern of the Experiment
%   directories.
% - DateTimes: name of a column in the MATs to parse as datetimes.
% - Index: name of a column in the MATs to use as an index.
% - StartDirectory: Path to the directory to search. By default searches
%   the current working directory.
% - Overwrite: a logical true/false whether to overwrite any existing
%   Parquet files. By default will skip subdirectories rather than
%   overwrite.
% - HighPrecision: a logical true/false whether to store numeric data in
%   the file as 64-bit floats and ints. By default, assumes that the MATs
%   are only accurate to 32 bit (~7dp). If set, the Parquet files will be
%   about twice as large.
% - Verbose: a logical true/false whether to print out all the skipped
%   directories.
% - MATPattern: Regular expression pattern to match for MAT filenames.
% - ParquetEngine: Engine to use for Parquet file write.
% - ParquetCompression: Type of compression to use for Parquet files.


%% Settings

% Default settings
DirectoryPattern = '*';
SubdirectoryPattern = '*';
DateTimes = [];
Index = [];
StartDirectory = '.';
Overwrite = false;
HighPrecision = false;
Verbose = false;
MATPattern = '*.mat'; % it is not sensitive to upper/lower case
ParquetEngine = 'fastparquet';
ParquetCompression = 'gzip'; % 'gzip' is ~20% slower than the default
                             % 'snappy', but ~2x smaller

% Settings from user
if nargin>1
    error('Unexpected number of inputs. Please pass options as a structure.');
elseif nargin==1
    if isfield(options,'DirectoryPattern')
        DirectoryPattern = options.DirectoryPattern;
    end
    if isfield(options,'SubdirectoryPattern')
        SubdirectoryPattern = options.SubdirectoryPattern;
    end
    if isfield(options,'DateTimes')
        DateTimes = options.DateTimes;
        DateTimes = regexprep(DateTimes, ' ', '_');
    end
    if isfield(options,'Index')
        Index = options.Index;
        Index = regexprep(Index, ' ', '_');
    end
    if isfield(options,'StartDirectory')
        StartDirectory = options.StartDirectory;
    end
    if isfield(options,'Overwrite')
        Overwrite = options.Overwrite;
    end
    if isfield(options,'HighPrecision')
        HighPrecision = options.HighPrecision;
    end
    if isfield(options,'Verbose')
        Verbose = options.Verbose;
    end
    if isfield(options,'MATPattern')
        MATPattern = options.MATPattern;
    end
    if isfield(options,'ParquetEngine')
        ParquetEngine = options.ParquetEngine;
    end
    if isfield(options,'ParquetCompression')
        ParquetCompression = options.ParquetCompression;
    end
end

%%
% We'll track the sizes to see how good the compression is
original_sizes = 0;
parquet_sizes = 0;
num_files_converted = 0;
num_files_skipped = 0;

% We'll also store any directories we found that weren't valid
empty_directories = [];

% Move to starting directory
CurrentDirectory = cd;
cd(StartDirectory);

% Get everything in the current directory matching our pattern then ignore
% everything that isn't a directory itself (or hidden!)
d_listing = dir(DirectoryPattern);
d_listing = d_listing(~startsWith({d_listing.name},'.'));
d_isd = [d_listing.isdir]; % returns logical vector
campaign_directories = {d_listing(d_isd).name};

for i = 1:length(campaign_directories)
    campaign_directory = campaign_directories{i};
    
    % Again find everything matching our pattern and ignore anything that
    % isn't a directory (e.g. the Parquet files!)
    sd_listing = dir([campaign_directory '\' SubdirectoryPattern]);
    sd_listing = sd_listing(~ismember({sd_listing.name},{'.','..'}));
    sd_isd = [sd_listing(:).isdir]; % returns logical vector
    experiment_directories = {sd_listing(sd_isd).name};
    
    if isempty(experiment_directories)
        empty_directories = [empty_directories, ...
                             [campaign_directory newline]];
        continue; % We skip this directory and go to the next one
    end
    
    for j = 1:length(experiment_directories)
        experiment_directory = experiment_directories{j};

        % Don't overwrite unless we're ordered to!
        parquet_filename = [campaign_directory '\' ...
                            experiment_directory '.parquet'];
        if isfile(parquet_filename) && ~Overwrite
            num_files_skipped = num_files_skipped+1;
            continue;
        end
        
        % Get a sorted list of all the MAT files within this directory 
        mat_listing = dir([campaign_directory '\' experiment_directory ...
                           '\' MATPattern]);
        if isempty(mat_listing)
            empty_directories = [empty_directories, ...
                                 [experiment_directory newline]];
            continue; % We skip this directory and go to the next one
        end
        mat_filenames = {mat_listing(:).name};
        mat_filenames = sort(mat_filenames);
        
        % Count the original size of these files and concatenate the data
        Table = table();
        for k = 1:length(mat_filenames)
            mat_filename = [campaign_directory '\' experiment_directory ...
                            '\' mat_filenames{k}];
            
            % Get size of mat file
            f_mat = dir(mat_filename);
            original_sizes = original_sizes+f_mat.bytes;
            
            % Concatenate tables
            data = load(mat_filename);
            field_names = fieldnames(data);
            for n = 1:length(field_names)
                T = getfield(data,field_names{n});
                if istable(T)
                    Table = [Table; T];
                end
            end
        end
        
        % Check that a table has been created
        if isempty(Table)
            warning([experiment_directory ' does not seem to contain any tables.']);
            continue;
        end
        
        % Remove the index column as the it is just row number so we don't
        % need to duplicate it
        if any(Index)
            try
                Table = removevars(Table,Index);
            catch
                warning([mat_filename ' does not seem to have a ' ...
                         'column called ' DateTimes '.']);
            end
        end
        
        % Convert all the numeric columns to smaller, unless we know
        % it's a good file
        if ~HighPrecision
            column_names = Table.Properties.VariableNames;
            if ~any(strcmp(column_names,DateTimes))
                warning([mat_filename ' does not seem to have a ' ...
                         'column called ' DateTimes '.']);
            end
            
            % Set the variable type for each column
            for m = 1:length(column_names)
                if strcmp(column_names{m},DateTimes)
                    try
                        Table = convertvars(Table, DateTimes, 'datetime');
                    catch
                    end
                else
                    try
                    	Table = convertvars(Table, column_names{m}, 'single');
                    catch
                    end
                end
            end
        end
        
        % Write the parquet file
        parquetwrite(parquet_filename,Table, ...
                     'Version','1.0', ...
                     'VariableCompression',ParquetCompression);
        num_files_converted = num_files_converted+1;
        
        % Get size of parquet file
        f_parquet = dir(parquet_filename);
        parquet_sizes = parquet_sizes+f_parquet.bytes;
        
    end
end

% Give final feedback
if num_files_converted>0
    disp([num2str(num_files_converted) ' converted file(s) are smaller ' ...
          'by a factor of ' num2str(original_sizes / parquet_sizes) '.']);
elseif ~Overwrite && num_files_skipped>0
    disp(['No files converted, but ' num2str(num_files_skipped) ...
          ' pre-existing Parquet file(s) skipped.']);
else
    disp('No files converted.');
end

% Log empty directories if requested
if Verbose && ~isempty(empty_directories)
    disp('Empty directories:');
    disp(empty_directories);
end

% Move back to original directory
cd(CurrentDirectory);


end
