using System;
using System.Windows.Forms;

namespace StreamDbExplorer
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void loadDbButton_Click(object sender, EventArgs e)
        {
            var result = openFileDialog1.ShowDialog();
            switch (result)
            {
                case DialogResult.Yes:
                case DialogResult.OK:
                    MessageBox.Show(openFileDialog1.FileName);
                    return;
                default: return;
            }
        }
    }
}
