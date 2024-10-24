#region License
// Copyright (c) 2016 Timothy Slater
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using TraceWizard.Data;

namespace TraceWizard.UI
{
    public partial class SQLViewer : Form
    {
        List<SQLStatement> sqlList;
        MainForm _mainForm;
        public SQLViewer(MainForm mainForm)
        {
            InitializeComponent();
            _mainForm = mainForm;
        }

        public SQLViewer(MainForm mainForm, List<SQLStatement> sqls, SQLByFrom byFrom) : this(mainForm)
        {
            sqlList = sqls.Where(p => p.FromClause.ToLower().Trim().Equals(byFrom.FromClause.ToLower().Trim())).ToList();
            InitUI();
        }

        public SQLViewer(MainForm mainForm, List<SQLStatement> sqls, SQLByWhere byWhere) : this(mainForm)
        {
            sqlList = sqls.Where(p => p.WhereClause.ToLower().Trim().Equals(byWhere.WhereClause.ToLower().Trim())).ToList();
            InitUI();
        }

        void InitUI()
        {
            UIBuilder.BuildAllSQLList(sqlListView, sqlList);
        }

        private void listView1_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (sqlListView.SelectedItems.Count > 0 && sqlListView.SelectedItems[0].Tag != null)
            {
                ItemExplainer.UpdateExplanation(listBox1, sqlListView.SelectedItems[0].Tag);
            }
        }

        private void listView1_MouseDoubleClick(object sender, MouseEventArgs e)
        {
            if (sqlListView.SelectedItems.Count > 0 && sqlListView.SelectedItems[0].Tag != null)
            {
                _mainForm.GoToSQLStatementInExecPath((SQLStatement)(sqlListView.SelectedItems[0].Tag));
            }
        }

        private void copyStatementToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var sqlStatement = sqlListView.SelectedItems[0].Tag as SQLStatement;

            Clipboard.SetText(sqlStatement.Statement);

            MessageBox.Show("SQL Statement copied to clipboard.");
        }

        private void copyBindsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var sqlStatement = sqlListView.SelectedItems[0].Tag as SQLStatement;

            StringBuilder sb = new StringBuilder();

            foreach (var b in sqlStatement.Executions[0].BindValues)
            {
                sb.AppendLine(String.Format("Bind {0} = {1}", b.Index, b.Value));
            }
            Clipboard.SetText(sb.ToString());

            MessageBox.Show("Bind values copied to clipboard.");
        }

        private void copyResolvedStatementToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var sqlStatement = sqlListView.SelectedItems[0].Tag as SQLStatement;

            string workingStatement = sqlStatement.Statement;

            foreach (var b in sqlStatement.Executions[0].BindValues.OrderBy(p => p.Index).Reverse())
            {
                workingStatement = workingStatement.Replace(":" + b.Index, "'" + b.Value + "'");
            }
            Clipboard.SetText(workingStatement);

            MessageBox.Show("Resolved statement copied to clipboard.");
        }

        private void sqlItemContextStrip_Opening(object sender, CancelEventArgs e)
        {
            if (sqlListView.SelectedItems.Count == 0)
            {
                e.Cancel = true;
                return;
            }
            var item = sqlListView.SelectedItems[0];

            if (item.Tag.GetType().Equals(typeof(SQLStatement)) == false)
            {
                e.Cancel = true;
                return;
            }

            var sqlStatment = item.Tag as SQLStatement;

            if (sqlStatment.Executions[0].BindValues.Count == 0)
            {
                copyBindsToolStripMenuItem.Enabled = false;
                copyResolvedStatementToolStripMenuItem.Enabled = false;
            }
            else
            {
                copyBindsToolStripMenuItem.Enabled = true;
                copyResolvedStatementToolStripMenuItem.Enabled = true;
            }


        }
    }
}
