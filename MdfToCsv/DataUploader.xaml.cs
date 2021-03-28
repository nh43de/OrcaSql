using System;
using System.CodeDom;
using System.Data;
using System.Threading.Tasks;
using System.Windows;
using DataPowerTools.Connectivity;
using DataPowerTools.DataConnectivity.Sql;
using DataPowerTools.DataReaderExtensibility.TransformingReaders;
using Microsoft.Data.SqlClient;
using Microsoft.Win32;

namespace DataToolChain
{
    /// <summary>
    ///     Interaction logic for DataUploader.xaml
    /// </summary>
    public partial class DataUploader : Window
    {
        public DataUploader()
        {
            InitializeComponent();
            DataContext = _viewModel;
        }

        public DataUploaderViewModel _viewModel { get; set; } = new DataUploaderViewModel();
        
        private void BrowseButton_Click(object sender, RoutedEventArgs e)
        {
            var a = new OpenFileDialog();
            a.Multiselect = true;

            if (a.ShowDialog() == true)
            {
                _viewModel.AddFiles(a.FileNames);
            }
        }

        private void ButtonCancel_Click(object sender, RoutedEventArgs e)
        {
            _viewModel.Cancel();
        }

        private void DataGrid_OnCurrentCellChanged(object sender, EventArgs e)
        {
            _viewModel.SaveConfig();
        }

        private void ButtonGo_Click(object sender, RoutedEventArgs e)
        {
            Task.Run(async () =>
            {
                try
                {
                    await _viewModel.Go();
                }
                catch (Exception ex)
                {
                    _viewModel.WindowStatusDisplay = "Finished with errors: " + ex.Message;
                }
            });
        }
    }
}
