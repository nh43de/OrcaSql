using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using DataPowerTools.Connectivity.Json;

namespace DataToolChain
{
    public class DataUploaderViewModel : INotifyPropertyChanged
    {
        private ObservableCollection<DataUploaderTask> _dataUploaderTasks = new ObservableCollection<DataUploaderTask>();
        
        private string _indexName = "";
        private string _outputFileName = "output.csv";
        private string _statusDisplay;
        private string _jsonConfiguration;
        private string _tableName = "MyTable";
        
        
        public DataUploaderViewModel()
        {
            _dataUploaderTasks.CollectionChanged += DataUploaderTasksOnCollectionChanged;
        }

        private void DataUploaderTasksOnCollectionChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            SaveConfig();
        }

        public string OutputFileName
        {
            get => _outputFileName;
            set
            {
                _outputFileName = value;
                OnPropertyChanged();
            }
        }

        public string TableName
        {
            get => _tableName;
            set
            {
                _tableName = value; 
                OnPropertyChanged();
            }
        }

        public int BulkCopyRowsPerBatch { get; set; } = 5000;
        
        public string IndexName
        {
            get => _indexName;
            set
            {
                _indexName = value;
                OnPropertyChanged();
            }
        }

        public string JsonConfiguration
        {
            get => _jsonConfiguration;
            set
            {
                _jsonConfiguration = value;
                LoadConfig();
                OnPropertyChanged();
            }
        }

        private class JsonConfig
        {
            public string OutputFileName { get; set; }
            public string TableName { get; set; }
            public string IndexName { get; set; }
            
            public DataUploaderTask[] Tasks { get; set; }
        }

        public void LoadConfig()
        {
            try
            {
                var a = JsonConfiguration.ToObject<JsonConfig>();
                OutputFileName = a.OutputFileName;
                TableName = a.TableName;
                IndexName = a.IndexName;

                DataUploaderTasks = new ObservableCollection<DataUploaderTask>(a.Tasks);
            }
            catch (Exception e)
            {
                //nah
            }
        }

        public void SaveConfig()
        {
            var a = new JsonConfig
            {
                OutputFileName = OutputFileName,
                TableName = TableName,
                IndexName = IndexName,
                Tasks = DataUploaderTasks.ToArray()
            };
            var b = a.ToJson(true);
            if (JsonConfiguration != b)
            {
                JsonConfiguration = b;
            }
        }
        
        public ObservableCollection<DataUploaderTask> DataUploaderTasks
        {
            get => _dataUploaderTasks;
            set
            {
                _dataUploaderTasks = value;
                OnPropertyChanged();
            }
        }

        public string WindowStatusDisplay
        {
            get => _statusDisplay;
            set
            {
                _statusDisplay = value;
                OnPropertyChanged();
            }
        }

        public Task CurrentTask { get; private set; }

        private CancellationTokenSource CancellationTokenSource { get; set; } = new CancellationTokenSource();

        public event PropertyChangedEventHandler PropertyChanged;

        public void AddFiles(IEnumerable<string> files)
        {
            foreach (var file in files)
            {
                AddFile(file);
            }
            SaveConfig();
        }

        public void AddFile(string file)
        {
            DataUploaderTasks.Add(new DataUploaderTask {FilePath = file});
        }

        public void Cancel()
        {
            CancellationTokenSource.Cancel();
            WindowStatusDisplay = "Operation cancelled";

            //TODO: this should be async and wait for the current task to complete (fail) before changing display
        }


        public async Task Go()
        {
            if (CurrentTask != null && !CurrentTask.IsCompleted)
            {
                MessageBox.Show("A process is currently running.");
                return;
            }

            DataUploaderTasks.ToList().ForEach(t =>
            {
                t.Success = false;
                t.StatusMessage = "";
            });

            var cancellationToken = CancellationTokenSource.Token;

            CurrentTask = null;

            WindowStatusDisplay = "Starting MDF file parsing.";
            






            CurrentTask = Task.Run(async () =>
            {
                foreach (var task in DataUploaderTasks)
                {
                    if (string.IsNullOrWhiteSpace(task.FilePath))
                    {
                        task.StatusMessage = "No file specified";
                        continue;
                    }
                    
                    try
                    {
                        var r = new MdfHelpers(this.DataUploaderTasks.Select(p => p.FilePath).ToArray());

                        var progress = new Progress<string>(newStr => this.WindowStatusDisplay = newStr);

                        r.Upload(progress, OutputFileName, TableName, IndexName);
                    }
                    catch (Exception e)
                    {
                        task.StatusMessage = "Error during upload: " + e.Message;
                        continue;
                    }
                }
            }, cancellationToken);

            CurrentTask?.GetAwaiter().OnCompleted(() =>
            {
                CurrentTask = null;
                WindowStatusDisplay = "Finished.";
            });
        }
        
        private string GetDestinationTable(DataUploaderTask task)
        {
            var destinationTable = string.IsNullOrWhiteSpace(task.DestinationTable) ? IndexName : task.DestinationTable;

            if (string.IsNullOrWhiteSpace(destinationTable))
            {
                //last check get from file name without extension
                destinationTable = System.IO.Path.GetFileNameWithoutExtension(task.FilePath);
            }

            return destinationTable;
        }


        private void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            SaveConfig();
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}