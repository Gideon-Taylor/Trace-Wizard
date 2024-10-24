using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using TraceWizard.Data;

namespace TraceWizard.UI
{
    public partial class CompareResult : Form
    {
        List<ExecutionCall> left;
        List<ExecutionCall> right;

        Dictionary<SQLStatement, TreeNode> lSQLMap = new Dictionary<SQLStatement, TreeNode>();
        Dictionary<ExecutionCall, TreeNode> lExecMap = new Dictionary<ExecutionCall, TreeNode>();

        Dictionary<SQLStatement, TreeNode> rSQLMap = new Dictionary<SQLStatement, TreeNode>();
        Dictionary<ExecutionCall, TreeNode> rExecMap = new Dictionary<ExecutionCall, TreeNode>();
        Dictionary<ExecutionCall, ExecutionCall> sameMap;
        public CompareResult(List<ExecutionCall> left, List<ExecutionCall> right, Dictionary<ExecutionCall, ExecutionCall> sameMap)
        {
            InitializeComponent();

            this.left = left;
            this.right = right;
            this.sameMap = sameMap;

            DrawTrees();
        }

        private void DrawTrees()
        {
            TraceData ltd = new TraceData();
            ltd.ExecutionPath = left;
            UIBuilder.BuildExecutionTree(ltd, diffLeft, lSQLMap, lExecMap, true, true);

            TraceData rtd = new TraceData();
            rtd.ExecutionPath = right;
            UIBuilder.BuildExecutionTree(rtd, diffRight, rSQLMap, rExecMap, true, true);
        }

        private void executionTree_BeforeExpand(object sender, TreeViewCancelEventArgs e)
        {
            var call = e.Node.Tag as ExecutionCall;

            if (e.Node.Nodes.Count == 1 && e.Node.Nodes[0].Text.Equals("Loading..."))
            {
                // build out nodes for call
                if (sender == diffLeft)
                {
                    leftBuildExpandingNode(e.Node, call);
                }
                else
                {
                    rightBuildExpandingNode(e.Node, call);
                }

            }
        }

        private void leftBuildExpandingNode(TreeNode node, ExecutionCall call)
        {
            node.Nodes.Clear();
            foreach (var child in call.Children)
            {
                UIBuilder.BuildExecutionTree(node, child, lSQLMap, lExecMap, true, true);
            }
        }

        private void rightBuildExpandingNode(TreeNode node, ExecutionCall call)
        {
            node.Nodes.Clear();
            foreach (var child in call.Children)
            {
                UIBuilder.BuildExecutionTree(node, child, rSQLMap, rExecMap, true, true);
            }
        }

        private void executionTree_AfterExpand(object sender, TreeViewEventArgs e)
        {
            var call = e.Node.Tag as ExecutionCall;
            if (e.Node.Parent == null)
            {
                /* root node */
                var firstNodeInRoot = e.Node.Nodes[0];
                var firstCall = firstNodeInRoot.Tag as ExecutionCall;
                TreeNode parentNode = null;
                if (sameMap.ContainsKey(firstCall))
                {
                    if (rExecMap.ContainsKey(sameMap[firstCall]))
                    {
                        parentNode = rExecMap[sameMap[firstCall]].Parent;
                    }
                    else if (rSQLMap.ContainsKey(sameMap[firstCall].SQLStatement))
                    {
                        parentNode = rSQLMap[sameMap[firstCall].SQLStatement].Parent;
                    }
                }
                else if (sameMap.ContainsValue(firstCall))
                {
                    firstCall = sameMap.Where(d => d.Value == firstCall).First().Key;

                    if (lExecMap.ContainsKey(firstCall))
                    {
                        parentNode = lExecMap[firstCall].Parent;
                    }
                    else if (lSQLMap.ContainsKey(firstCall.SQLStatement))
                    {
                        parentNode = lSQLMap[firstCall.SQLStatement].Parent;
                    }
                }
                if (parentNode != null && parentNode.IsExpanded == false)
                {
                    parentNode.Expand();
                }
            }

            if (call != null && sameMap.ContainsKey(call))
            {
                /* clicked on left side */
                var rightCall = sameMap[call];

                /* find right tree node */
                var node = rExecMap[rightCall];
                if (node.IsExpanded == false)
                {
                    node.Expand();
                    diffRight.SelectedNode = node;
                }
            }

            if (call != null && sameMap.ContainsValue(call))
            {
                /* clicked on right side */
                var leftCall = sameMap.Where(d => d.Value == call).First().Key;
                /* find right tree node */
                var node = lExecMap[leftCall];
                if (node.IsExpanded == false)
                {
                    node.Expand();
                    diffLeft.SelectedNode = node;
                }
            }
        }

        private void executionTree_AfterSelect(object sender, TreeViewEventArgs e)
        {
            if (e.Node.Tag != null)
            {
                ItemExplainer.UpdateExplanation(itemDetail, e.Node.Tag);

                var call = e.Node.Tag as ExecutionCall;

                if (sameMap.ContainsKey(call))
                {
                    call = sameMap[call];
                }
                else if (sameMap.ContainsValue(call))
                {
                    call = sameMap.Where(m => m.Value == call).First().Key;
                }
                else
                {
                    return;
                }

                Dictionary<ExecutionCall, TreeNode> execMap = (sender == diffLeft ? rExecMap : lExecMap);
                Dictionary<SQLStatement, TreeNode> sqlMap = (sender == diffLeft ? rSQLMap : lSQLMap);
                TreeView otherView = (sender == diffLeft ? diffRight : diffLeft);

                if (call.Type == ExecutionCallType.SQL)
                {
                    /* use SQL Map */
                    otherView.SelectedNode = sqlMap[call.SQLStatement];
                }
                else
                {
                    /* use Exec Map */
                    otherView.SelectedNode = execMap[call];
                }
            }
        }

        private void execution_NodeClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (e.Button != MouseButtons.Left)
            {
                if (e.Node.Tag != null && (e.Node.Tag as ExecutionCall).Type != ExecutionCallType.SQL)
                {
                    executionContextStrip.Tag = e.Node.Tag;
                    executionContextStrip.Show(sender as Control, e.Location);

                }
                else if (e.Node.Tag != null && (e.Node.Tag as ExecutionCall).Type == ExecutionCallType.SQL)
                {
                    sqlItemContextStrip.Tag = e.Node.Tag;
                    sqlItemContextStrip.Show(sender as Control, e.Location);
                }
            }
        }

        private void copyStackTraceToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var item = executionContextStrip.Tag as ExecutionCall;
            if (item != null)
            {
                var stackTrace = item.GetStackTrace();

                StringBuilder sb = new StringBuilder();

                for (var x = 0; x < stackTrace.Count; x++)
                {
                    sb.Append(new string(' ', 4 * x));
                    sb.AppendLine(String.Format("{0}", stackTrace[x].Item2));
                }

                if (MainForm.IsRunningOSX)
                {
                    OSXClipboard.CopyToClipboard(sb.ToString());
                }
                else
                {
                    Clipboard.SetText(sb.ToString());
                }

                MessageBox.Show("Stack trace copied successfully.");
            }
        }

        private void copyResolvedStatementToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var execCall = sqlItemContextStrip.Tag as ExecutionCall;
            var sqlStatement = execCall.SQLStatement;

            string workingStatement = sqlStatement.Statement;

            foreach (var b in sqlStatement.Executions[0].BindValues.OrderBy(p => p.Index).Reverse())
            {
                var valueString = "";
                if (b.Type == 19)
                {
                    valueString = b.Value;
                }
                else
                {
                    valueString = "'" + b.Value + "'";
                }
                workingStatement = workingStatement.Replace(":" + b.Index, valueString);
            }

            if (MainForm.IsRunningOSX)
            {
                OSXClipboard.CopyToClipboard(workingStatement);
            }
            else
            {
                Clipboard.SetText(workingStatement);
            }

            MessageBox.Show("Resolved statement copied to clipboard.");
        }

        private void copyStatementToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var execCall = sqlItemContextStrip.Tag as ExecutionCall;
            var sqlStatement = execCall.SQLStatement;

            if (MainForm.IsRunningOSX)
            {
                OSXClipboard.CopyToClipboard(sqlStatement.Statement);
            }
            else
            {
                Clipboard.SetText(sqlStatement.Statement);
            }

            MessageBox.Show("SQL Statement copied to clipboard.");
        }

        private void copyBindsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var execCall = sqlItemContextStrip.Tag as ExecutionCall;
            var sqlStatement = execCall.SQLStatement;

            StringBuilder sb = new StringBuilder();

            foreach (var b in sqlStatement.Executions[0].BindValues)
            {
                sb.AppendLine(String.Format("Bind {0} = {1}", b.Index, b.Value));
            }

            if (MainForm.IsRunningOSX)
            {
                OSXClipboard.CopyToClipboard(sb.ToString());
            }
            else
            {
                Clipboard.SetText(sb.ToString());
            }

            MessageBox.Show("Bind values copied to clipboard.");
        }

        private void copyStackTraceToolStripMenuItem1_Click(object sender, EventArgs e)
        {
            var item = sqlItemContextStrip.Tag as ExecutionCall;
            if (item != null)
            {
                var stackTrace = item.GetStackTrace();

                StringBuilder sb = new StringBuilder();

                for (var x = 0; x < stackTrace.Count; x++)
                {
                    sb.Append(new string(' ', 4 * x));
                    sb.AppendLine(String.Format("{0}", stackTrace[x].Item2));
                }

                if (MainForm.IsRunningOSX)
                {
                    OSXClipboard.CopyToClipboard(sb.ToString());
                }
                else
                {
                    Clipboard.SetText(sb.ToString());
                }

                MessageBox.Show("Stack trace copied successfully.");
            }
        }
    }
}
