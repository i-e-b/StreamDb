namespace StreamDbExplorer
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.loadDbButton = new System.Windows.Forms.Button();
            this.pathListBox = new System.Windows.Forms.ListBox();
            this.inspectStructureButton = new System.Windows.Forms.Button();
            this.dbFilePathLabel = new System.Windows.Forms.Label();
            this.openFileDialog1 = new System.Windows.Forms.OpenFileDialog();
            this.scanChecksumsButton = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // loadDbButton
            // 
            this.loadDbButton.Location = new System.Drawing.Point(12, 12);
            this.loadDbButton.Name = "loadDbButton";
            this.loadDbButton.Size = new System.Drawing.Size(129, 23);
            this.loadDbButton.TabIndex = 0;
            this.loadDbButton.Text = "Load Database";
            this.loadDbButton.UseVisualStyleBackColor = true;
            this.loadDbButton.Click += new System.EventHandler(this.loadDbButton_Click);
            // 
            // pathListBox
            // 
            this.pathListBox.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.pathListBox.FormattingEnabled = true;
            this.pathListBox.Location = new System.Drawing.Point(12, 41);
            this.pathListBox.Name = "pathListBox";
            this.pathListBox.Size = new System.Drawing.Size(776, 394);
            this.pathListBox.TabIndex = 1;
            // 
            // inspectStructureButton
            // 
            this.inspectStructureButton.Location = new System.Drawing.Point(147, 12);
            this.inspectStructureButton.Name = "inspectStructureButton";
            this.inspectStructureButton.Size = new System.Drawing.Size(105, 23);
            this.inspectStructureButton.TabIndex = 2;
            this.inspectStructureButton.Text = "Inspect Structure";
            this.inspectStructureButton.UseVisualStyleBackColor = true;
            // 
            // dbFilePathLabel
            // 
            this.dbFilePathLabel.AutoSize = true;
            this.dbFilePathLabel.Location = new System.Drawing.Point(369, 17);
            this.dbFilePathLabel.Name = "dbFilePathLabel";
            this.dbFilePathLabel.Size = new System.Drawing.Size(71, 13);
            this.dbFilePathLabel.TabIndex = 3;
            this.dbFilePathLabel.Text = "<db file path>";
            // 
            // openFileDialog1
            // 
            this.openFileDialog1.FileName = "openFileDialog1";
            this.openFileDialog1.Title = "Load a StreamDB file";
            // 
            // scanChecksumsButton
            // 
            this.scanChecksumsButton.Location = new System.Drawing.Point(258, 12);
            this.scanChecksumsButton.Name = "scanChecksumsButton";
            this.scanChecksumsButton.Size = new System.Drawing.Size(105, 23);
            this.scanChecksumsButton.TabIndex = 4;
            this.scanChecksumsButton.Text = "Scan checksums";
            this.scanChecksumsButton.UseVisualStyleBackColor = true;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.scanChecksumsButton);
            this.Controls.Add(this.dbFilePathLabel);
            this.Controls.Add(this.inspectStructureButton);
            this.Controls.Add(this.pathListBox);
            this.Controls.Add(this.loadDbButton);
            this.Name = "Form1";
            this.Text = "StreamDB inspector";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button loadDbButton;
        private System.Windows.Forms.ListBox pathListBox;
        private System.Windows.Forms.Button inspectStructureButton;
        private System.Windows.Forms.Label dbFilePathLabel;
        private System.Windows.Forms.OpenFileDialog openFileDialog1;
        private System.Windows.Forms.Button scanChecksumsButton;
    }
}

