namespace TraceWizard
{
    partial class MainForm
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
            this.components = new System.ComponentModel.Container();
            this.menuStrip = new System.Windows.Forms.MenuStrip();
            this.fileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.openToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.openInNewWindowToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.saveAsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator3 = new System.Windows.Forms.ToolStripSeparator();
            this.compareTracesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator4 = new System.Windows.Forms.ToolStripSeparator();
            this.exitToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.settingsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.viewToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.sQLToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.allToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.byWhereClauseToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.byFromClauseToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.errorsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.tablesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.execUnfoldAllMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.execGoToSlowestToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.generateSQLScriptToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.helpToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.checkForUpdateToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.aboutToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.openFileDialog1 = new System.Windows.Forms.OpenFileDialog();
            this.statusStrip = new System.Windows.Forms.StatusStrip();
            this.progressBar = new System.Windows.Forms.ToolStripProgressBar();
            this.memoryUsage = new System.Windows.Forms.ToolStripStatusLabel();
            this.memoryUsageTimer = new System.Windows.Forms.Timer(this.components);
            this.sqlItemContextStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.copyResolvedStatementToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.copyStatementToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.copyBindsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator2 = new System.Windows.Forms.ToolStripSeparator();
            this.copyStackTraceToolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.copyLineNumberToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.saveFileDialog1 = new System.Windows.Forms.SaveFileDialog();
            this.executionContextStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.copyStackTraceToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.copySQLStatementToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.showAEBuffer = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator5 = new System.Windows.Forms.ToolStripSeparator();
            this.copyCallStartLineToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.BottomToolStripPanel = new System.Windows.Forms.ToolStripPanel();
            this.TopToolStripPanel = new System.Windows.Forms.ToolStripPanel();
            this.RightToolStripPanel = new System.Windows.Forms.ToolStripPanel();
            this.LeftToolStripPanel = new System.Windows.Forms.ToolStripPanel();
            this.ContentPanel = new System.Windows.Forms.ToolStripContentPanel();
            this.splitContainer1 = new System.Windows.Forms.SplitContainer();
            this.mainTabStrip = new System.Windows.Forms.TabControl();
            this.StatsTab = new System.Windows.Forms.TabPage();
            this.executionPathTab = new System.Windows.Forms.TabPage();
            this.execFindBox = new System.Windows.Forms.TextBox();
            this.executionTree = new System.Windows.Forms.TreeView();
            this.sqlStatementsTab = new System.Windows.Forms.TabPage();
            this.sqlFindBox = new System.Windows.Forms.TextBox();
            this.sqlListView = new System.Windows.Forms.ListView();
            this.stackTraceTab = new System.Windows.Forms.TabPage();
            this.stackTraceFindBox = new System.Windows.Forms.TextBox();
            this.stackTraceListView = new System.Windows.Forms.ListView();
            this.ppcObjectTab = new System.Windows.Forms.TabPage();
            this.ppcObjectTree = new System.Windows.Forms.TreeView();
            this.ppcObjectFindBox = new System.Windows.Forms.TextBox();
            this.variablesTab = new System.Windows.Forms.TabPage();
            this.lstVariables = new System.Windows.Forms.ListView();
            this.detailsBox = new System.Windows.Forms.ListBox();
            this.detailsContextStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.copyDetailsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripContainer1 = new System.Windows.Forms.ToolStripContainer();
            this.saveFileDialog2 = new System.Windows.Forms.SaveFileDialog();
            this.saveFileDialog3 = new System.Windows.Forms.SaveFileDialog();
            this.menuStrip.SuspendLayout();
            this.statusStrip.SuspendLayout();
            this.sqlItemContextStrip.SuspendLayout();
            this.executionContextStrip.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).BeginInit();
            this.splitContainer1.Panel1.SuspendLayout();
            this.splitContainer1.Panel2.SuspendLayout();
            this.splitContainer1.SuspendLayout();
            this.mainTabStrip.SuspendLayout();
            this.executionPathTab.SuspendLayout();
            this.sqlStatementsTab.SuspendLayout();
            this.stackTraceTab.SuspendLayout();
            this.ppcObjectTab.SuspendLayout();
            this.variablesTab.SuspendLayout();
            this.detailsContextStrip.SuspendLayout();
            this.toolStripContainer1.ContentPanel.SuspendLayout();
            this.toolStripContainer1.SuspendLayout();
            this.SuspendLayout();
            // 
            // menuStrip
            // 
            this.menuStrip.ImageScalingSize = new System.Drawing.Size(32, 32);
            this.menuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.fileToolStripMenuItem,
            this.editToolStripMenuItem,
            this.viewToolStripMenuItem,
            this.toolsToolStripMenuItem,
            this.helpToolStripMenuItem});
            this.menuStrip.Location = new System.Drawing.Point(0, 0);
            this.menuStrip.Name = "menuStrip";
            this.menuStrip.Size = new System.Drawing.Size(1030, 24);
            this.menuStrip.TabIndex = 0;
            this.menuStrip.Text = "menuStrip1";
            // 
            // fileToolStripMenuItem
            // 
            this.fileToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.openToolStripMenuItem,
            this.openInNewWindowToolStripMenuItem,
            this.toolStripSeparator1,
            this.saveAsToolStripMenuItem,
            this.toolStripSeparator3,
            this.compareTracesToolStripMenuItem,
            this.toolStripSeparator4,
            this.exitToolStripMenuItem});
            this.fileToolStripMenuItem.Name = "fileToolStripMenuItem";
            this.fileToolStripMenuItem.Size = new System.Drawing.Size(37, 20);
            this.fileToolStripMenuItem.Text = "&File";
            // 
            // openToolStripMenuItem
            // 
            this.openToolStripMenuItem.Name = "openToolStripMenuItem";
            this.openToolStripMenuItem.ShortcutKeyDisplayString = "";
            this.openToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.O)));
            this.openToolStripMenuItem.Size = new System.Drawing.Size(190, 22);
            this.openToolStripMenuItem.Text = "&Open...";
            this.openToolStripMenuItem.Click += new System.EventHandler(this.openToolStripMenuItem_Click);
            // 
            // openInNewWindowToolStripMenuItem
            // 
            this.openInNewWindowToolStripMenuItem.Name = "openInNewWindowToolStripMenuItem";
            this.openInNewWindowToolStripMenuItem.Size = new System.Drawing.Size(190, 22);
            this.openInNewWindowToolStripMenuItem.Text = "Open in New Window";
            this.openInNewWindowToolStripMenuItem.Click += new System.EventHandler(this.openInNewWindowToolStripMenuItem_Click);
            // 
            // toolStripSeparator1
            // 
            this.toolStripSeparator1.Name = "toolStripSeparator1";
            this.toolStripSeparator1.Size = new System.Drawing.Size(187, 6);
            // 
            // saveAsToolStripMenuItem
            // 
            this.saveAsToolStripMenuItem.Name = "saveAsToolStripMenuItem";
            this.saveAsToolStripMenuItem.Size = new System.Drawing.Size(190, 22);
            this.saveAsToolStripMenuItem.Text = "Save As...";
            this.saveAsToolStripMenuItem.Click += new System.EventHandler(this.saveAsToolStripMenuItem_Click);
            // 
            // toolStripSeparator3
            // 
            this.toolStripSeparator3.Name = "toolStripSeparator3";
            this.toolStripSeparator3.Size = new System.Drawing.Size(187, 6);
            // 
            // compareTracesToolStripMenuItem
            // 
            this.compareTracesToolStripMenuItem.Name = "compareTracesToolStripMenuItem";
            this.compareTracesToolStripMenuItem.Size = new System.Drawing.Size(190, 22);
            this.compareTracesToolStripMenuItem.Text = "Compare Traces...";
            this.compareTracesToolStripMenuItem.Click += new System.EventHandler(this.compareTracesToolStripMenuItem_Click);
            // 
            // toolStripSeparator4
            // 
            this.toolStripSeparator4.Name = "toolStripSeparator4";
            this.toolStripSeparator4.Size = new System.Drawing.Size(187, 6);
            // 
            // exitToolStripMenuItem
            // 
            this.exitToolStripMenuItem.Name = "exitToolStripMenuItem";
            this.exitToolStripMenuItem.ShortcutKeyDisplayString = "";
            this.exitToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.X)));
            this.exitToolStripMenuItem.Size = new System.Drawing.Size(190, 22);
            this.exitToolStripMenuItem.Text = "E&xit";
            this.exitToolStripMenuItem.Click += new System.EventHandler(this.exitToolStripMenuItem_Click);
            // 
            // editToolStripMenuItem
            // 
            this.editToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.settingsToolStripMenuItem});
            this.editToolStripMenuItem.Name = "editToolStripMenuItem";
            this.editToolStripMenuItem.Size = new System.Drawing.Size(39, 20);
            this.editToolStripMenuItem.Text = "&Edit";
            // 
            // settingsToolStripMenuItem
            // 
            this.settingsToolStripMenuItem.Name = "settingsToolStripMenuItem";
            this.settingsToolStripMenuItem.Size = new System.Drawing.Size(125, 22);
            this.settingsToolStripMenuItem.Text = "&Settings...";
            this.settingsToolStripMenuItem.Click += new System.EventHandler(this.settingsToolStripMenuItem_Click);
            // 
            // viewToolStripMenuItem
            // 
            this.viewToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.sQLToolStripMenuItem,
            this.execUnfoldAllMenuItem,
            this.execGoToSlowestToolStripMenuItem});
            this.viewToolStripMenuItem.Name = "viewToolStripMenuItem";
            this.viewToolStripMenuItem.Size = new System.Drawing.Size(44, 20);
            this.viewToolStripMenuItem.Text = "&View";
            this.viewToolStripMenuItem.DropDownOpening += new System.EventHandler(this.viewToolStripMenuItem_DropDownOpening);
            // 
            // sQLToolStripMenuItem
            // 
            this.sQLToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.allToolStripMenuItem,
            this.byWhereClauseToolStripMenuItem,
            this.byFromClauseToolStripMenuItem,
            this.errorsToolStripMenuItem,
            this.tablesToolStripMenuItem});
            this.sQLToolStripMenuItem.Name = "sQLToolStripMenuItem";
            this.sQLToolStripMenuItem.Size = new System.Drawing.Size(147, 22);
            this.sQLToolStripMenuItem.Text = "S&QL";
            // 
            // allToolStripMenuItem
            // 
            this.allToolStripMenuItem.CheckOnClick = true;
            this.allToolStripMenuItem.Name = "allToolStripMenuItem";
            this.allToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Alt | System.Windows.Forms.Keys.A)));
            this.allToolStripMenuItem.Size = new System.Drawing.Size(203, 22);
            this.allToolStripMenuItem.Text = "&All";
            this.allToolStripMenuItem.Click += new System.EventHandler(this.SQLView_Clicked);
            // 
            // byWhereClauseToolStripMenuItem
            // 
            this.byWhereClauseToolStripMenuItem.CheckOnClick = true;
            this.byWhereClauseToolStripMenuItem.Name = "byWhereClauseToolStripMenuItem";
            this.byWhereClauseToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Alt | System.Windows.Forms.Keys.W)));
            this.byWhereClauseToolStripMenuItem.Size = new System.Drawing.Size(203, 22);
            this.byWhereClauseToolStripMenuItem.Text = "By &Where Clause";
            this.byWhereClauseToolStripMenuItem.Click += new System.EventHandler(this.SQLView_Clicked);
            // 
            // byFromClauseToolStripMenuItem
            // 
            this.byFromClauseToolStripMenuItem.CheckOnClick = true;
            this.byFromClauseToolStripMenuItem.Name = "byFromClauseToolStripMenuItem";
            this.byFromClauseToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Alt | System.Windows.Forms.Keys.F)));
            this.byFromClauseToolStripMenuItem.Size = new System.Drawing.Size(203, 22);
            this.byFromClauseToolStripMenuItem.Text = "By &From Clause";
            this.byFromClauseToolStripMenuItem.Click += new System.EventHandler(this.SQLView_Clicked);
            // 
            // errorsToolStripMenuItem
            // 
            this.errorsToolStripMenuItem.CheckOnClick = true;
            this.errorsToolStripMenuItem.Name = "errorsToolStripMenuItem";
            this.errorsToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Alt | System.Windows.Forms.Keys.E)));
            this.errorsToolStripMenuItem.Size = new System.Drawing.Size(203, 22);
            this.errorsToolStripMenuItem.Text = "&Errors";
            this.errorsToolStripMenuItem.Click += new System.EventHandler(this.SQLView_Clicked);
            // 
            // tablesToolStripMenuItem
            // 
            this.tablesToolStripMenuItem.Name = "tablesToolStripMenuItem";
            this.tablesToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Alt | System.Windows.Forms.Keys.T)));
            this.tablesToolStripMenuItem.Size = new System.Drawing.Size(203, 22);
            this.tablesToolStripMenuItem.Text = "Tables";
            this.tablesToolStripMenuItem.Click += new System.EventHandler(this.SQLView_Clicked);
            // 
            // execUnfoldAllMenuItem
            // 
            this.execUnfoldAllMenuItem.Name = "execUnfoldAllMenuItem";
            this.execUnfoldAllMenuItem.Size = new System.Drawing.Size(147, 22);
            this.execUnfoldAllMenuItem.Text = "Unfold All";
            this.execUnfoldAllMenuItem.Click += new System.EventHandler(this.execUnfoldAllMenuItem_Click);
            // 
            // execGoToSlowestToolStripMenuItem
            // 
            this.execGoToSlowestToolStripMenuItem.Name = "execGoToSlowestToolStripMenuItem";
            this.execGoToSlowestToolStripMenuItem.Size = new System.Drawing.Size(147, 22);
            this.execGoToSlowestToolStripMenuItem.Text = "Go To Slowest";
            this.execGoToSlowestToolStripMenuItem.Click += new System.EventHandler(this.execGoToSlowestToolStripMenuItem_Click);
            // 
            // toolsToolStripMenuItem
            // 
            this.toolsToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.generateSQLScriptToolStripMenuItem});
            this.toolsToolStripMenuItem.Name = "toolsToolStripMenuItem";
            this.toolsToolStripMenuItem.Size = new System.Drawing.Size(46, 20);
            this.toolsToolStripMenuItem.Text = "Tools";
            // 
            // generateSQLScriptToolStripMenuItem
            // 
            this.generateSQLScriptToolStripMenuItem.Name = "generateSQLScriptToolStripMenuItem";
            this.generateSQLScriptToolStripMenuItem.Size = new System.Drawing.Size(178, 22);
            this.generateSQLScriptToolStripMenuItem.Text = "Generate SQL Script";
            this.generateSQLScriptToolStripMenuItem.Click += new System.EventHandler(this.generateSQLScriptToolStripMenuItem_Click);
            // 
            // helpToolStripMenuItem
            // 
            this.helpToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.checkForUpdateToolStripMenuItem,
            this.aboutToolStripMenuItem});
            this.helpToolStripMenuItem.Name = "helpToolStripMenuItem";
            this.helpToolStripMenuItem.Size = new System.Drawing.Size(44, 20);
            this.helpToolStripMenuItem.Text = "&Help";
            // 
            // checkForUpdateToolStripMenuItem
            // 
            this.checkForUpdateToolStripMenuItem.Name = "checkForUpdateToolStripMenuItem";
            this.checkForUpdateToolStripMenuItem.Size = new System.Drawing.Size(165, 22);
            this.checkForUpdateToolStripMenuItem.Text = "Check for update";
            this.checkForUpdateToolStripMenuItem.Click += new System.EventHandler(this.checkForUpdateToolStripMenuItem_Click);
            // 
            // aboutToolStripMenuItem
            // 
            this.aboutToolStripMenuItem.Name = "aboutToolStripMenuItem";
            this.aboutToolStripMenuItem.Size = new System.Drawing.Size(165, 22);
            this.aboutToolStripMenuItem.Text = "&About";
            this.aboutToolStripMenuItem.Click += new System.EventHandler(this.aboutToolStripMenuItem_Click);
            // 
            // openFileDialog1
            // 
            this.openFileDialog1.FileName = "openFileDialog1";
            // 
            // statusStrip
            // 
            this.statusStrip.ImageScalingSize = new System.Drawing.Size(32, 32);
            this.statusStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.progressBar,
            this.memoryUsage});
            this.statusStrip.Location = new System.Drawing.Point(0, 521);
            this.statusStrip.Name = "statusStrip";
            this.statusStrip.Size = new System.Drawing.Size(1030, 22);
            this.statusStrip.TabIndex = 1;
            this.statusStrip.Text = "statusStrip1";
            // 
            // progressBar
            // 
            this.progressBar.AutoSize = false;
            this.progressBar.Minimum = 100;
            this.progressBar.Name = "progressBar";
            this.progressBar.Size = new System.Drawing.Size(100, 16);
            this.progressBar.Step = 1;
            this.progressBar.Value = 100;
            // 
            // memoryUsage
            // 
            this.memoryUsage.Name = "memoryUsage";
            this.memoryUsage.Size = new System.Drawing.Size(100, 17);
            this.memoryUsage.Text = "Max: 100 Cur: 100";
            // 
            // memoryUsageTimer
            // 
            this.memoryUsageTimer.Enabled = true;
            this.memoryUsageTimer.Interval = 1000;
            this.memoryUsageTimer.Tick += new System.EventHandler(this.timer1_Tick);
            // 
            // sqlItemContextStrip
            // 
            this.sqlItemContextStrip.ImageScalingSize = new System.Drawing.Size(32, 32);
            this.sqlItemContextStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.copyResolvedStatementToolStripMenuItem,
            this.copyStatementToolStripMenuItem,
            this.copyBindsToolStripMenuItem,
            this.toolStripSeparator2,
            this.copyStackTraceToolStripMenuItem1,
            this.copyLineNumberToolStripMenuItem});
            this.sqlItemContextStrip.Name = "sqlItemContextStrip";
            this.sqlItemContextStrip.Size = new System.Drawing.Size(210, 120);
            this.sqlItemContextStrip.Opening += new System.ComponentModel.CancelEventHandler(this.sqlItemContextStrip_Opening);
            // 
            // copyResolvedStatementToolStripMenuItem
            // 
            this.copyResolvedStatementToolStripMenuItem.Name = "copyResolvedStatementToolStripMenuItem";
            this.copyResolvedStatementToolStripMenuItem.Size = new System.Drawing.Size(209, 22);
            this.copyResolvedStatementToolStripMenuItem.Text = "Copy Resolved Statement";
            this.copyResolvedStatementToolStripMenuItem.Click += new System.EventHandler(this.copyResolvedStatementToolStripMenuItem_Click);
            // 
            // copyStatementToolStripMenuItem
            // 
            this.copyStatementToolStripMenuItem.Name = "copyStatementToolStripMenuItem";
            this.copyStatementToolStripMenuItem.Size = new System.Drawing.Size(209, 22);
            this.copyStatementToolStripMenuItem.Text = "Copy Statement";
            this.copyStatementToolStripMenuItem.Click += new System.EventHandler(this.copyStatementToolStripMenuItem_Click);
            // 
            // copyBindsToolStripMenuItem
            // 
            this.copyBindsToolStripMenuItem.Name = "copyBindsToolStripMenuItem";
            this.copyBindsToolStripMenuItem.Size = new System.Drawing.Size(209, 22);
            this.copyBindsToolStripMenuItem.Text = "Copy Binds";
            this.copyBindsToolStripMenuItem.Click += new System.EventHandler(this.copyBindsToolStripMenuItem_Click);
            // 
            // toolStripSeparator2
            // 
            this.toolStripSeparator2.Name = "toolStripSeparator2";
            this.toolStripSeparator2.Size = new System.Drawing.Size(206, 6);
            // 
            // copyStackTraceToolStripMenuItem1
            // 
            this.copyStackTraceToolStripMenuItem1.Name = "copyStackTraceToolStripMenuItem1";
            this.copyStackTraceToolStripMenuItem1.Size = new System.Drawing.Size(209, 22);
            this.copyStackTraceToolStripMenuItem1.Text = "Copy Stack Trace";
            this.copyStackTraceToolStripMenuItem1.Click += new System.EventHandler(this.copyStackTraceToolStripMenuItem1_Click);
            // 
            // copyLineNumberToolStripMenuItem
            // 
            this.copyLineNumberToolStripMenuItem.Name = "copyLineNumberToolStripMenuItem";
            this.copyLineNumberToolStripMenuItem.Size = new System.Drawing.Size(209, 22);
            this.copyLineNumberToolStripMenuItem.Text = "Copy Line Number";
            this.copyLineNumberToolStripMenuItem.Click += new System.EventHandler(this.copyLineNumberToolStripMenuItem_Click);
            // 
            // saveFileDialog1
            // 
            this.saveFileDialog1.Filter = "Trace Wizard Files|*.twiz";
            // 
            // executionContextStrip
            // 
            this.executionContextStrip.ImageScalingSize = new System.Drawing.Size(32, 32);
            this.executionContextStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.copyStackTraceToolStripMenuItem,
            this.copySQLStatementToolStripMenuItem,
            this.showAEBuffer,
            this.toolStripSeparator5,
            this.copyCallStartLineToolStripMenuItem});
            this.executionContextStrip.Name = "executionContextStrip";
            this.executionContextStrip.Size = new System.Drawing.Size(216, 98);
            this.executionContextStrip.Opening += new System.ComponentModel.CancelEventHandler(this.executionContextStrip_Opening);
            // 
            // copyStackTraceToolStripMenuItem
            // 
            this.copyStackTraceToolStripMenuItem.Name = "copyStackTraceToolStripMenuItem";
            this.copyStackTraceToolStripMenuItem.Size = new System.Drawing.Size(215, 22);
            this.copyStackTraceToolStripMenuItem.Text = "Copy Stack Trace";
            this.copyStackTraceToolStripMenuItem.Click += new System.EventHandler(this.copyStackTraceToolStripMenuItem_Click);
            // 
            // copySQLStatementToolStripMenuItem
            // 
            this.copySQLStatementToolStripMenuItem.Name = "copySQLStatementToolStripMenuItem";
            this.copySQLStatementToolStripMenuItem.Size = new System.Drawing.Size(215, 22);
            this.copySQLStatementToolStripMenuItem.Text = "Copy SQL Statement";
            this.copySQLStatementToolStripMenuItem.Visible = false;
            this.copySQLStatementToolStripMenuItem.Click += new System.EventHandler(this.copySQLStatementToolStripMenuItem_Click);
            // 
            // showAEBuffer
            // 
            this.showAEBuffer.Name = "showAEBuffer";
            this.showAEBuffer.Size = new System.Drawing.Size(215, 22);
            this.showAEBuffer.Text = "Show Current State Record";
            this.showAEBuffer.Visible = false;
            this.showAEBuffer.Click += new System.EventHandler(this.showAEBuffer_Click);
            // 
            // toolStripSeparator5
            // 
            this.toolStripSeparator5.Name = "toolStripSeparator5";
            this.toolStripSeparator5.Size = new System.Drawing.Size(212, 6);
            // 
            // copyCallStartLineToolStripMenuItem
            // 
            this.copyCallStartLineToolStripMenuItem.Name = "copyCallStartLineToolStripMenuItem";
            this.copyCallStartLineToolStripMenuItem.Size = new System.Drawing.Size(215, 22);
            this.copyCallStartLineToolStripMenuItem.Text = "Copy Line Number";
            this.copyCallStartLineToolStripMenuItem.Click += new System.EventHandler(this.copyCallStartLineToolStripMenuItem_Click);
            // 
            // BottomToolStripPanel
            // 
            this.BottomToolStripPanel.Location = new System.Drawing.Point(0, 0);
            this.BottomToolStripPanel.Name = "BottomToolStripPanel";
            this.BottomToolStripPanel.Orientation = System.Windows.Forms.Orientation.Horizontal;
            this.BottomToolStripPanel.RowMargin = new System.Windows.Forms.Padding(3, 0, 0, 0);
            this.BottomToolStripPanel.Size = new System.Drawing.Size(0, 0);
            // 
            // TopToolStripPanel
            // 
            this.TopToolStripPanel.Location = new System.Drawing.Point(0, 0);
            this.TopToolStripPanel.Name = "TopToolStripPanel";
            this.TopToolStripPanel.Orientation = System.Windows.Forms.Orientation.Horizontal;
            this.TopToolStripPanel.RowMargin = new System.Windows.Forms.Padding(3, 0, 0, 0);
            this.TopToolStripPanel.Size = new System.Drawing.Size(0, 0);
            // 
            // RightToolStripPanel
            // 
            this.RightToolStripPanel.Location = new System.Drawing.Point(0, 0);
            this.RightToolStripPanel.Name = "RightToolStripPanel";
            this.RightToolStripPanel.Orientation = System.Windows.Forms.Orientation.Horizontal;
            this.RightToolStripPanel.RowMargin = new System.Windows.Forms.Padding(3, 0, 0, 0);
            this.RightToolStripPanel.Size = new System.Drawing.Size(0, 0);
            // 
            // LeftToolStripPanel
            // 
            this.LeftToolStripPanel.Location = new System.Drawing.Point(0, 0);
            this.LeftToolStripPanel.Name = "LeftToolStripPanel";
            this.LeftToolStripPanel.Orientation = System.Windows.Forms.Orientation.Horizontal;
            this.LeftToolStripPanel.RowMargin = new System.Windows.Forms.Padding(3, 0, 0, 0);
            this.LeftToolStripPanel.Size = new System.Drawing.Size(0, 0);
            // 
            // ContentPanel
            // 
            this.ContentPanel.Size = new System.Drawing.Size(1030, 552);
            // 
            // splitContainer1
            // 
            this.splitContainer1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer1.Location = new System.Drawing.Point(0, 0);
            this.splitContainer1.Name = "splitContainer1";
            this.splitContainer1.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // splitContainer1.Panel1
            // 
            this.splitContainer1.Panel1.Controls.Add(this.mainTabStrip);
            // 
            // splitContainer1.Panel2
            // 
            this.splitContainer1.Panel2.Controls.Add(this.detailsBox);
            this.splitContainer1.Size = new System.Drawing.Size(1030, 472);
            this.splitContainer1.SplitterDistance = 376;
            this.splitContainer1.TabIndex = 3;
            // 
            // mainTabStrip
            // 
            this.mainTabStrip.Controls.Add(this.StatsTab);
            this.mainTabStrip.Controls.Add(this.executionPathTab);
            this.mainTabStrip.Controls.Add(this.sqlStatementsTab);
            this.mainTabStrip.Controls.Add(this.stackTraceTab);
            this.mainTabStrip.Controls.Add(this.ppcObjectTab);
            this.mainTabStrip.Controls.Add(this.variablesTab);
            this.mainTabStrip.Dock = System.Windows.Forms.DockStyle.Fill;
            this.mainTabStrip.Location = new System.Drawing.Point(0, 0);
            this.mainTabStrip.Name = "mainTabStrip";
            this.mainTabStrip.SelectedIndex = 0;
            this.mainTabStrip.Size = new System.Drawing.Size(1030, 376);
            this.mainTabStrip.TabIndex = 3;
            this.mainTabStrip.Selected += new System.Windows.Forms.TabControlEventHandler(this.mainTabStrip_Selected);
            this.mainTabStrip.KeyDown += new System.Windows.Forms.KeyEventHandler(this.mainTabStrip_KeyDown);
            this.mainTabStrip.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.MainForm_KeyPress);
            // 
            // StatsTab
            // 
            this.StatsTab.Location = new System.Drawing.Point(4, 22);
            this.StatsTab.Name = "StatsTab";
            this.StatsTab.Padding = new System.Windows.Forms.Padding(3);
            this.StatsTab.Size = new System.Drawing.Size(1022, 350);
            this.StatsTab.TabIndex = 0;
            this.StatsTab.Text = "Statistics";
            this.StatsTab.UseVisualStyleBackColor = true;
            // 
            // executionPathTab
            // 
            this.executionPathTab.Controls.Add(this.execFindBox);
            this.executionPathTab.Controls.Add(this.executionTree);
            this.executionPathTab.Location = new System.Drawing.Point(4, 22);
            this.executionPathTab.Name = "executionPathTab";
            this.executionPathTab.Padding = new System.Windows.Forms.Padding(3);
            this.executionPathTab.Size = new System.Drawing.Size(1022, 350);
            this.executionPathTab.TabIndex = 1;
            this.executionPathTab.Text = "Execution Path";
            this.executionPathTab.UseVisualStyleBackColor = true;
            // 
            // execFindBox
            // 
            this.execFindBox.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.execFindBox.Location = new System.Drawing.Point(3, 327);
            this.execFindBox.Name = "execFindBox";
            this.execFindBox.Size = new System.Drawing.Size(1016, 20);
            this.execFindBox.TabIndex = 2;
            this.execFindBox.Visible = false;
            this.execFindBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.executionSearchKeyDown);
            // 
            // executionTree
            // 
            this.executionTree.Dock = System.Windows.Forms.DockStyle.Fill;
            this.executionTree.HideSelection = false;
            this.executionTree.Location = new System.Drawing.Point(3, 3);
            this.executionTree.Name = "executionTree";
            this.executionTree.Size = new System.Drawing.Size(1016, 344);
            this.executionTree.TabIndex = 1;
            this.executionTree.BeforeExpand += new System.Windows.Forms.TreeViewCancelEventHandler(this.executionTree_BeforeExpand);
            this.executionTree.NodeMouseClick += new System.Windows.Forms.TreeNodeMouseClickEventHandler(this.executionTree_NodeMouseClick);
            this.executionTree.NodeMouseDoubleClick += new System.Windows.Forms.TreeNodeMouseClickEventHandler(this.executionTree_NodeMouseDoubleClick);
            this.executionTree.KeyDown += new System.Windows.Forms.KeyEventHandler(this.executionSearchKeyDown);
            // 
            // sqlStatementsTab
            // 
            this.sqlStatementsTab.Controls.Add(this.sqlFindBox);
            this.sqlStatementsTab.Controls.Add(this.sqlListView);
            this.sqlStatementsTab.Location = new System.Drawing.Point(4, 22);
            this.sqlStatementsTab.Name = "sqlStatementsTab";
            this.sqlStatementsTab.Padding = new System.Windows.Forms.Padding(3);
            this.sqlStatementsTab.Size = new System.Drawing.Size(1022, 350);
            this.sqlStatementsTab.TabIndex = 2;
            this.sqlStatementsTab.Text = "SQL Statements";
            this.sqlStatementsTab.UseVisualStyleBackColor = true;
            // 
            // sqlFindBox
            // 
            this.sqlFindBox.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.sqlFindBox.Location = new System.Drawing.Point(3, 327);
            this.sqlFindBox.Name = "sqlFindBox";
            this.sqlFindBox.Size = new System.Drawing.Size(1016, 20);
            this.sqlFindBox.TabIndex = 3;
            this.sqlFindBox.Visible = false;
            this.sqlFindBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.sqlFindBox_KeyDown);
            // 
            // sqlListView
            // 
            this.sqlListView.ContextMenuStrip = this.sqlItemContextStrip;
            this.sqlListView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.sqlListView.Font = new System.Drawing.Font("Verdana", 9F);
            this.sqlListView.FullRowSelect = true;
            this.sqlListView.GridLines = true;
            this.sqlListView.HideSelection = false;
            this.sqlListView.Location = new System.Drawing.Point(3, 3);
            this.sqlListView.MultiSelect = false;
            this.sqlListView.Name = "sqlListView";
            this.sqlListView.Size = new System.Drawing.Size(1016, 344);
            this.sqlListView.TabIndex = 0;
            this.sqlListView.UseCompatibleStateImageBehavior = false;
            this.sqlListView.View = System.Windows.Forms.View.Details;
            this.sqlListView.ColumnClick += new System.Windows.Forms.ColumnClickEventHandler(this.listView1_ColumnClick);
            this.sqlListView.SelectedIndexChanged += new System.EventHandler(this.listView1_SelectedIndexChanged);
            this.sqlListView.KeyDown += new System.Windows.Forms.KeyEventHandler(this.sqlFindBox_KeyDown);
            this.sqlListView.MouseDoubleClick += new System.Windows.Forms.MouseEventHandler(this.listView1_MouseDoubleClick);
            // 
            // stackTraceTab
            // 
            this.stackTraceTab.Controls.Add(this.stackTraceFindBox);
            this.stackTraceTab.Controls.Add(this.stackTraceListView);
            this.stackTraceTab.Location = new System.Drawing.Point(4, 22);
            this.stackTraceTab.Name = "stackTraceTab";
            this.stackTraceTab.Padding = new System.Windows.Forms.Padding(3);
            this.stackTraceTab.Size = new System.Drawing.Size(1022, 350);
            this.stackTraceTab.TabIndex = 3;
            this.stackTraceTab.Text = "Stack Traces";
            this.stackTraceTab.UseVisualStyleBackColor = true;
            // 
            // stackTraceFindBox
            // 
            this.stackTraceFindBox.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.stackTraceFindBox.Location = new System.Drawing.Point(3, 327);
            this.stackTraceFindBox.Name = "stackTraceFindBox";
            this.stackTraceFindBox.Size = new System.Drawing.Size(1016, 20);
            this.stackTraceFindBox.TabIndex = 4;
            this.stackTraceFindBox.Visible = false;
            // 
            // stackTraceListView
            // 
            this.stackTraceListView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.stackTraceListView.Font = new System.Drawing.Font("Verdana", 9F);
            this.stackTraceListView.FullRowSelect = true;
            this.stackTraceListView.GridLines = true;
            this.stackTraceListView.HideSelection = false;
            this.stackTraceListView.Location = new System.Drawing.Point(3, 3);
            this.stackTraceListView.MultiSelect = false;
            this.stackTraceListView.Name = "stackTraceListView";
            this.stackTraceListView.Size = new System.Drawing.Size(1016, 344);
            this.stackTraceListView.TabIndex = 1;
            this.stackTraceListView.UseCompatibleStateImageBehavior = false;
            this.stackTraceListView.View = System.Windows.Forms.View.Details;
            this.stackTraceListView.SelectedIndexChanged += new System.EventHandler(this.listView1_SelectedIndexChanged);
            this.stackTraceListView.KeyDown += new System.Windows.Forms.KeyEventHandler(this.stackTraceListView_KeyDown);
            this.stackTraceListView.MouseDoubleClick += new System.Windows.Forms.MouseEventHandler(this.stackTraceListView_MouseDoubleClick);
            // 
            // ppcObjectTab
            // 
            this.ppcObjectTab.Controls.Add(this.ppcObjectTree);
            this.ppcObjectTab.Controls.Add(this.ppcObjectFindBox);
            this.ppcObjectTab.Location = new System.Drawing.Point(4, 22);
            this.ppcObjectTab.Name = "ppcObjectTab";
            this.ppcObjectTab.Padding = new System.Windows.Forms.Padding(3);
            this.ppcObjectTab.Size = new System.Drawing.Size(1022, 350);
            this.ppcObjectTab.TabIndex = 4;
            this.ppcObjectTab.Text = "PeopleCode Objects";
            this.ppcObjectTab.UseVisualStyleBackColor = true;
            // 
            // ppcObjectTree
            // 
            this.ppcObjectTree.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ppcObjectTree.HideSelection = false;
            this.ppcObjectTree.Location = new System.Drawing.Point(3, 3);
            this.ppcObjectTree.Name = "ppcObjectTree";
            this.ppcObjectTree.Size = new System.Drawing.Size(1016, 324);
            this.ppcObjectTree.TabIndex = 5;
            this.ppcObjectTree.NodeMouseDoubleClick += new System.Windows.Forms.TreeNodeMouseClickEventHandler(this.ppcObjectTree_NodeMouseDoubleClick);
            // 
            // ppcObjectFindBox
            // 
            this.ppcObjectFindBox.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.ppcObjectFindBox.Location = new System.Drawing.Point(3, 327);
            this.ppcObjectFindBox.Name = "ppcObjectFindBox";
            this.ppcObjectFindBox.Size = new System.Drawing.Size(1016, 20);
            this.ppcObjectFindBox.TabIndex = 4;
            this.ppcObjectFindBox.Visible = false;
            // 
            // variablesTab
            // 
            this.variablesTab.Controls.Add(this.lstVariables);
            this.variablesTab.Location = new System.Drawing.Point(4, 22);
            this.variablesTab.Name = "variablesTab";
            this.variablesTab.Padding = new System.Windows.Forms.Padding(3);
            this.variablesTab.Size = new System.Drawing.Size(1022, 350);
            this.variablesTab.TabIndex = 5;
            this.variablesTab.Text = "Global/Component Vars";
            this.variablesTab.UseVisualStyleBackColor = true;
            // 
            // lstVariables
            // 
            this.lstVariables.Dock = System.Windows.Forms.DockStyle.Fill;
            this.lstVariables.Font = new System.Drawing.Font("Verdana", 9F);
            this.lstVariables.FullRowSelect = true;
            this.lstVariables.GridLines = true;
            this.lstVariables.HideSelection = false;
            this.lstVariables.Location = new System.Drawing.Point(3, 3);
            this.lstVariables.MultiSelect = false;
            this.lstVariables.Name = "lstVariables";
            this.lstVariables.Size = new System.Drawing.Size(1016, 344);
            this.lstVariables.TabIndex = 2;
            this.lstVariables.UseCompatibleStateImageBehavior = false;
            this.lstVariables.View = System.Windows.Forms.View.Details;
            // 
            // detailsBox
            // 
            this.detailsBox.ContextMenuStrip = this.detailsContextStrip;
            this.detailsBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.detailsBox.FormattingEnabled = true;
            this.detailsBox.HorizontalScrollbar = true;
            this.detailsBox.Location = new System.Drawing.Point(0, 0);
            this.detailsBox.Name = "detailsBox";
            this.detailsBox.Size = new System.Drawing.Size(1030, 92);
            this.detailsBox.TabIndex = 0;
            // 
            // detailsContextStrip
            // 
            this.detailsContextStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.copyDetailsToolStripMenuItem});
            this.detailsContextStrip.Name = "detailsContextStrip";
            this.detailsContextStrip.Size = new System.Drawing.Size(141, 26);
            // 
            // copyDetailsToolStripMenuItem
            // 
            this.copyDetailsToolStripMenuItem.Name = "copyDetailsToolStripMenuItem";
            this.copyDetailsToolStripMenuItem.Size = new System.Drawing.Size(140, 22);
            this.copyDetailsToolStripMenuItem.Text = "Copy Details";
            this.copyDetailsToolStripMenuItem.Click += new System.EventHandler(this.copyDetailsToolStripMenuItem_Click);
            // 
            // toolStripContainer1
            // 
            this.toolStripContainer1.BottomToolStripPanelVisible = false;
            // 
            // toolStripContainer1.ContentPanel
            // 
            this.toolStripContainer1.ContentPanel.Controls.Add(this.splitContainer1);
            this.toolStripContainer1.ContentPanel.Size = new System.Drawing.Size(1030, 472);
            this.toolStripContainer1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.toolStripContainer1.LeftToolStripPanelVisible = false;
            this.toolStripContainer1.Location = new System.Drawing.Point(0, 24);
            this.toolStripContainer1.Name = "toolStripContainer1";
            this.toolStripContainer1.RightToolStripPanelVisible = false;
            this.toolStripContainer1.Size = new System.Drawing.Size(1030, 497);
            this.toolStripContainer1.TabIndex = 2;
            this.toolStripContainer1.Text = "toolStripContainer1";
            // 
            // MainForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1030, 543);
            this.Controls.Add(this.toolStripContainer1);
            this.Controls.Add(this.statusStrip);
            this.Controls.Add(this.menuStrip);
            this.MainMenuStrip = this.menuStrip;
            this.Name = "MainForm";
            this.Text = "Trace Wizard";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.Shown += new System.EventHandler(this.MainForm_Shown);
            this.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.MainForm_KeyPress);
            this.Resize += new System.EventHandler(this.MainForm_Resize);
            this.menuStrip.ResumeLayout(false);
            this.menuStrip.PerformLayout();
            this.statusStrip.ResumeLayout(false);
            this.statusStrip.PerformLayout();
            this.sqlItemContextStrip.ResumeLayout(false);
            this.executionContextStrip.ResumeLayout(false);
            this.splitContainer1.Panel1.ResumeLayout(false);
            this.splitContainer1.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).EndInit();
            this.splitContainer1.ResumeLayout(false);
            this.mainTabStrip.ResumeLayout(false);
            this.executionPathTab.ResumeLayout(false);
            this.executionPathTab.PerformLayout();
            this.sqlStatementsTab.ResumeLayout(false);
            this.sqlStatementsTab.PerformLayout();
            this.stackTraceTab.ResumeLayout(false);
            this.stackTraceTab.PerformLayout();
            this.ppcObjectTab.ResumeLayout(false);
            this.ppcObjectTab.PerformLayout();
            this.variablesTab.ResumeLayout(false);
            this.detailsContextStrip.ResumeLayout(false);
            this.toolStripContainer1.ContentPanel.ResumeLayout(false);
            this.toolStripContainer1.ResumeLayout(false);
            this.toolStripContainer1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip menuStrip;
        private System.Windows.Forms.ToolStripMenuItem fileToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem openToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem exitToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem helpToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem aboutToolStripMenuItem;
        private System.Windows.Forms.OpenFileDialog openFileDialog1;
        private System.Windows.Forms.StatusStrip statusStrip;
        private System.Windows.Forms.ToolStripProgressBar progressBar;
        private System.Windows.Forms.ToolStripStatusLabel memoryUsage;
        private System.Windows.Forms.Timer memoryUsageTimer;
        private System.Windows.Forms.ToolStripMenuItem editToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem settingsToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator1;
        private System.Windows.Forms.SaveFileDialog saveFileDialog1;
        private System.Windows.Forms.ContextMenuStrip sqlItemContextStrip;
        private System.Windows.Forms.ToolStripMenuItem copyStatementToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem copyBindsToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem copyResolvedStatementToolStripMenuItem;
        private System.Windows.Forms.ContextMenuStrip executionContextStrip;
        private System.Windows.Forms.ToolStripMenuItem copyStackTraceToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem copyStackTraceToolStripMenuItem1;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator2;
        private System.Windows.Forms.ToolStripMenuItem viewToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem sQLToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem allToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem byWhereClauseToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem byFromClauseToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem errorsToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem openInNewWindowToolStripMenuItem;
        private System.Windows.Forms.ToolStripPanel BottomToolStripPanel;
        private System.Windows.Forms.ToolStripPanel TopToolStripPanel;
        private System.Windows.Forms.ToolStripPanel RightToolStripPanel;
        private System.Windows.Forms.ToolStripPanel LeftToolStripPanel;
        private System.Windows.Forms.ToolStripContentPanel ContentPanel;
        private System.Windows.Forms.SplitContainer splitContainer1;
        private System.Windows.Forms.TabControl mainTabStrip;
        private System.Windows.Forms.TabPage StatsTab;
        private System.Windows.Forms.TabPage executionPathTab;
        private System.Windows.Forms.TextBox execFindBox;
        private System.Windows.Forms.TreeView executionTree;
        private System.Windows.Forms.TabPage sqlStatementsTab;
        private System.Windows.Forms.TextBox sqlFindBox;
        private System.Windows.Forms.ListView sqlListView;
        private System.Windows.Forms.TabPage stackTraceTab;
        private System.Windows.Forms.TextBox stackTraceFindBox;
        private System.Windows.Forms.ListView stackTraceListView;
        private System.Windows.Forms.ListBox detailsBox;
        private System.Windows.Forms.ToolStripContainer toolStripContainer1;
        private System.Windows.Forms.ToolStripMenuItem saveAsToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator3;
        private System.Windows.Forms.SaveFileDialog saveFileDialog2;
        private System.Windows.Forms.ToolStripMenuItem tablesToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem compareTracesToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator4;
        private System.Windows.Forms.ToolStripMenuItem checkForUpdateToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem toolsToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem generateSQLScriptToolStripMenuItem;
        private System.Windows.Forms.SaveFileDialog saveFileDialog3;
        private System.Windows.Forms.ContextMenuStrip detailsContextStrip;
        private System.Windows.Forms.ToolStripMenuItem copyDetailsToolStripMenuItem;
        private System.Windows.Forms.TabPage ppcObjectTab;
        private System.Windows.Forms.TreeView ppcObjectTree;
        private System.Windows.Forms.TextBox ppcObjectFindBox;
        private System.Windows.Forms.ToolStripMenuItem execUnfoldAllMenuItem;
        private System.Windows.Forms.ToolStripMenuItem execGoToSlowestToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem showAEBuffer;
        private System.Windows.Forms.ToolStripMenuItem copySQLStatementToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem copyLineNumberToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator5;
        private System.Windows.Forms.ToolStripMenuItem copyCallStartLineToolStripMenuItem;
        private System.Windows.Forms.TabPage variablesTab;
        private System.Windows.Forms.ListView lstVariables;
    }
}

