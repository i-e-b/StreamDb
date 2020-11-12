#nullable enable
using System;
using System.IO;
using System.Linq;
using System.Windows.Forms;
using StreamDb;

namespace StreamDbExplorer
{
    public partial class Form1 : Form
    {
        private Database? _loaded;
        
        public Form1()
        {
            InitializeComponent();
        }

        private void TryLoadDatabase(string? path)
        {
            pathListBox!.Items.Clear();
            if (string.IsNullOrWhiteSpace(path!)) return;
            if (! File.Exists(path!)) return;

            try
            {
                _loaded = Database.TryConnect(File.Open(path!, FileMode.Open, FileAccess.Read));
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString(),"Failed to open database");
            }

            _loaded!.CalculateStatistics(out var pages, out var free);
            dbFilePathLabel!.Text = $"{path} ▕▔▏   {free} pages free of {pages}"; 
            ReloadPaths();
        }

        private void ReloadPaths()
        {
            pathListBox!.Items.Clear();
            pathListBox.Items.AddRange(_loaded!.Search("").Select(
                path=>(object)(path + " -> " + _loaded.GetDocumentInfo(path))
            ).ToArray());
        }

        private void loadDbButton_Click(object sender, EventArgs e)
        {
            var result = openFileDialog1!.ShowDialog();
            switch (result)
            {
                case DialogResult.Yes:
                case DialogResult.OK:
                    TryLoadDatabase(openFileDialog1.FileName);
                    return;
                default: return;
            }
        }
    }
}
