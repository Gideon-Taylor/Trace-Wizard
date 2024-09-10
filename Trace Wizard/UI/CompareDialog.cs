using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using TraceWizard.Data;
using TraceWizard.Data.Serialization;
using TraceWizard.Processors;
using BasicSQLFormatter;
using System.Diagnostics;
namespace TraceWizard.UI
{
    public partial class CompareDialog : Form
    {
        TraceData leftData;
        TraceData rightData;
        BackgroundWorker leftProcessor;
        BackgroundWorker rightProcessor;

        Dictionary<SQLStatement, TreeNode> SQLMap = new Dictionary<SQLStatement, TreeNode>();
        Dictionary<ExecutionCall, TreeNode> ExecMap = new Dictionary<ExecutionCall, TreeNode>();

        Dictionary<int, ExecutionCall> leftSelected = new Dictionary<int, ExecutionCall>();
        Dictionary<int, ExecutionCall> rightSelected = new Dictionary<int, ExecutionCall>();


        public CompareDialog(TraceData initData)
        {
            InitializeComponent();

            if (initData != null)
            {
                leftData = initData;
                btnCopyToRight.Enabled = true;
                btnLeftSelectAll.Enabled = true;

                UIBuilder.BuildExecutionTree(leftData, execTreeLeft, SQLMap, ExecMap, false);
            }
        }

        private void btnOpenLeft_Click(object sender, EventArgs e)
        {
            openFileDialog1.Filter = "Trace Files|*.tracesql;*.twiz;*.trc;*.aet";
            openFileDialog1.FileName = "";
            var result = openFileDialog1.ShowDialog();
            if (result == DialogResult.Cancel)
            {
                return;
            }
            btnLeftSelectAll.Enabled = false;
            execTreeLeft.Nodes.Clear();
            leftSelected.Clear();

            if (result == DialogResult.OK)
            {
                string filename = openFileDialog1.FileName;
                // Process the file

                if (TraceDeserializer.IsSerializedData(filename))
                {
                    if (TraceDeserializer.IsSerializedVersionSupported(filename))
                    {
                        leftData = new TraceDeserializer().DeserializeTraceData(new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.None));
                        UIBuilder.BuildExecutionTree(leftData, execTreeLeft, SQLMap, ExecMap, false);
                        btnLeftSelectAll.Enabled = true;
                        btnCopyToRight.Enabled = true;
                        if (leftData != null && rightData != null)
                        {
                            btnCompare.Enabled = true;
                        }
                        System.GC.Collect();
                    }
                    else
                    {
                        MessageBox.Show("This version of Trace Wizard cannot import the serialized data, maybe it was serialized with an older version.");
                    }
                }
                else
                {

                    var fileExtension = new FileInfo(filename).Extension.ToLower();

                    if (fileExtension.Equals(".tracesql") || fileExtension.Equals(".trc"))
                    {
                        leftProcessor = new TraceSQLProcessor(filename);
                    } else if (fileExtension.Equals(".aet"))
                    {
                        leftProcessor = new AETTraceProcessor(filename);
                    }

                    leftProcessor.WorkerReportsProgress = true;
                    leftProcessor.WorkerSupportsCancellation = true;

                    leftProcessor.ProgressChanged += ProcessorLeft_ProgressChanged;
                    leftProcessor.RunWorkerCompleted += ProcessorLeft_RunWorkerCompleted;
                    leftProcessor.RunWorkerAsync();
                }
            }
        }
        private void ProcessorLeft_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            leftProgress.Value = e.ProgressPercentage;
        }
        private void ProcessorLeft_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            leftProgress.Value = 0;
            if (e.Cancelled)
            {
                MessageBox.Show("Trace file processing cancelled.");
                System.GC.Collect();
                return;
            }

            leftData = (TraceData)e.Result;

            UIBuilder.BuildExecutionTree(leftData, execTreeLeft, SQLMap, ExecMap, false);

            btnCopyToRight.Enabled = true;
            btnLeftSelectAll.Enabled = true;
            if (leftData != null && rightData != null)
            {
                btnCompare.Enabled = true;
            }

            System.GC.Collect();
        }

        private void btnOpenRight_Click(object sender, EventArgs e)
        {
            
            openFileDialog1.Filter = "Trace Files|*.tracesql;*.twiz;*.trc;*.aet";
            openFileDialog1.FileName = "";
            var result = openFileDialog1.ShowDialog();
            if (result == DialogResult.Cancel)
            {
                return;
            }

            execTreeRight.Nodes.Clear();
            rightSelected.Clear();
            btnRightSelectAll.Enabled = false;

            if (result == DialogResult.OK)
            {
                string filename = openFileDialog1.FileName;
                // Process the file

                if (TraceDeserializer.IsSerializedData(filename))
                {
                    if (TraceDeserializer.IsSerializedVersionSupported(filename))
                    {
                        rightData = new TraceDeserializer().DeserializeTraceData(new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.None));
                        UIBuilder.BuildExecutionTree(rightData, execTreeRight, SQLMap, ExecMap, false);
                        btnRightSelectAll.Enabled = true;
                        if (leftData != null && rightData != null)
                        {
                            btnCompare.Enabled = true;
                        }
                        System.GC.Collect();
                    }
                    else
                    {
                        MessageBox.Show("This version of Trace Wizard cannot import the serialized data, maybe it was serialized with an older version.");
                    }
                }
                else
                {
                    var fileExtension = new FileInfo(filename).Extension.ToLower();

                    if (fileExtension.Equals(".tracesql") || fileExtension.Equals(".trc"))
                    {
                        rightProcessor = new TraceSQLProcessor(filename);

                        /* Read first line and check to see if this is an AE Tracesql */
                        var or = File.OpenRead(filename);
                        var sr = new StreamReader(or);
                        var firstLine = sr.ReadLine();
                        if (firstLine.Contains("AE SQL/PeopleCode Trace"))
                        {
                            ((TraceSQLProcessor)rightProcessor).IsAETrace = true;
                        }
                        sr.Close();

                    }
                    else if (fileExtension.Equals(".aet"))
                    {
                        rightProcessor = new AETTraceProcessor(filename);
                    }
                    
                    rightProcessor.WorkerReportsProgress = true;
                    rightProcessor.WorkerSupportsCancellation = true;

                    rightProcessor.ProgressChanged += ProcessorRight_ProgressChanged;
                    rightProcessor.RunWorkerCompleted += ProcessorRight_RunWorkerCompleted;
                    rightProcessor.RunWorkerAsync();
                }
            }
        }
        private void ProcessorRight_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            rightProgress.Value = e.ProgressPercentage;
        }
        private void ProcessorRight_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            rightProgress.Value = 0;
            if (e.Cancelled)
            {
                MessageBox.Show("Trace file processing cancelled.");
                System.GC.Collect();
                return;
            }

            rightData = (TraceData)e.Result;

            UIBuilder.BuildExecutionTree(rightData, execTreeRight, SQLMap, ExecMap, false);
            btnRightSelectAll.Enabled = true;
            if (leftData != null && rightData != null)
            {
                btnCompare.Enabled = true;
            }

            System.GC.Collect();
        }
        
        private void btnCopyToRight_Click(object sender, EventArgs e)
        {
            rightData = leftData;
            UIBuilder.BuildExecutionTree(rightData, execTreeRight, SQLMap, ExecMap, false);
            btnRightSelectAll.Enabled = true;
            if (leftData != null && rightData != null)
            {
                btnCompare.Enabled = true;
            }
        }

        private void execTree_AfterCheck(object sender, TreeViewEventArgs e)
        {
            var treeView = sender as TreeView;
            Dictionary<int,ExecutionCall> list = (treeView == execTreeLeft ? leftSelected : rightSelected);

            var node = e.Node;

            if (node.Parent == null)
            {
                foreach (TreeNode child in node.Nodes)
                {
                    child.Checked = node.Checked;
                    
                }
                /* Context level was clicked */
                } else
            {
                if (node.Checked)
                {
                    /* selected */
                    /* determine index */
                    var indexInContext = node.Index;

                    var itemsAbove = 0;
                    var context = node.Parent;
                    var parentIndex = context.Index;

                    for (var x = 0; x < parentIndex; x++)
                    {
                        
                        itemsAbove += treeView.Nodes[x].Nodes.Count;
                    }

                    list.Add(itemsAbove + indexInContext, node.Tag as ExecutionCall);
                } else
                {
                    var indexInContext = node.Index;

                    var itemsAbove = 0;
                    var context = node.Parent;
                    var parentIndex = context.Index;

                    for (var x = 0; x < parentIndex; x++)
                    {
                        itemsAbove += treeView.Nodes[x].Nodes.Count;
                    }
                    list.Remove(itemsAbove + indexInContext);
                }
            }
            
        }

        private void btnCompare_Click(object sender, EventArgs e)
        {
            /* Check to make sure items on both sides have been picked */
            if (leftSelected.Count == 0 || rightSelected.Count == 0)
            {
                MessageBox.Show("You must select items on both sides before comparing.");
                return;
            }

            /* Generate XML files for left and right */
            List<ExecutionCall> leftList = leftSelected.OrderBy(p => p.Key).Select(p => p.Value).ToList<ExecutionCall>();
            List<ExecutionCall> rightList = rightSelected.OrderBy(p => p.Key).Select(p => p.Value).ToList<ExecutionCall>();

            var map = PathDiff.GeneratePathDiff(leftList, rightList);

            new CompareResult(leftList, rightList,map).Show(this);

        }

        private void btnLeftSelectAll_Click(object sender, EventArgs e)
        {
            foreach (TreeNode n in execTreeLeft.Nodes)
            {
                n.Checked = true;
            }
        }

        private void btnRightSelectAll_Click(object sender, EventArgs e)
        {
            foreach (TreeNode n in execTreeRight.Nodes)
            {
                n.Checked = true;
            }
        }
    }
}
