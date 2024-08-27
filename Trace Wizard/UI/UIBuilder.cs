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
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Windows.Forms;
using TraceWizard.Data;

namespace TraceWizard.UI
{
    public class UIBuilder
    {
        public static ListView view;
        public static TabControl BuildStatisticsPage(List<StatisticItem> items, MouseEventHandler doubleClickCallback)
        {
            var statGroups = items.GroupBy(s => s.Category);
            TabControl ctrl = new TabControl();
            List<TabPage> pages = new List<TabPage>();
            ctrl.Dock = DockStyle.Fill;
            foreach (var group in statGroups.OrderBy(g => g.Key))
            {
                var page = new TabPage(group.Key);
                ctrl.TabPages.Add(page);
                pages.Add(page);
                var listView = new ListView();
                UIBuilder.view = listView;
                page.Controls.Add(listView);
                listView.Dock = System.Windows.Forms.DockStyle.Fill;
                listView.Font = new System.Drawing.Font("Verdana", 9F);
                listView.FullRowSelect = true;
                listView.GridLines = true;
                listView.HideSelection = false;
                listView.Location = new System.Drawing.Point(3, 3);
                listView.UseCompatibleStateImageBehavior = false;
                listView.View = System.Windows.Forms.View.Details;
                listView.Columns.Add("Name");
                listView.Columns.Add("Value");
                listView.Columns.Add("Extra");

                listView.MouseDoubleClick += doubleClickCallback;

                foreach (var item in group)
                {
                    var listItem = new ListViewItem(item.Label);
                    listItem.SubItems.Add(item.Value);
                    listItem.SubItems.Add(item.Tag?.ToString());
                    listView.Items.Add(listItem);
                    listItem.Tag = item.Tag;
                }
                foreach (var c in listView.Columns)
                {
                    ((ColumnHeader)c).AutoResize(ColumnHeaderAutoResizeStyle.ColumnContent);
                }    
            }

            ctrl.SelectedIndex = 0;

            return ctrl;
        }
        
        public static void BuildAllSQLList(ListView view, List<SQLStatement> sqls)
        {
            view.BeginUpdate();
            view.Columns.Clear();
            view.Items.Clear();

            view.Columns.Add("Line #");
            view.Columns.Add("Duration");
            view.Columns.Add("Fetches");
            view.Columns.Add("SQL_ID");
            view.Columns.Add("Full SQL");

            view.ListViewItemSorter = new ListViewItemComparer(0,true);
            /* sort the SQLs */
            //sqls.Sort(new Comparison<SQLStatement>(sort));

            foreach (var sql in sqls)
            {
                ListViewItem item = new ListViewItem();
                if (sql.IsError)
                {
                    item.BackColor = System.Drawing.Color.Red;
                }
                item.Tag = sql;
                item.Text = sql.LineNumber.ToString();
                item.SubItems.Add(sql.Duration.ToString());
                item.SubItems.Add(sql.FetchCount.ToString());
                item.SubItems.Add(sql.SQLID);
                item.SubItems.Add(sql.Statement);

                view.Items.Add(item);
            }

            foreach (ColumnHeader header in view.Columns)
            {
                if (header.Text == "Fetches")
                {
                    header.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
                }
                else
                {
                    header.AutoResize(ColumnHeaderAutoResizeStyle.ColumnContent);
                }
                
            }
            view.EndUpdate();
        }

        public static void BuildWhereSQLList(ListView view, List<SQLByWhere> sqls)
        {
            view.BeginUpdate();
            view.Columns.Clear();
            view.Items.Clear();

            var totalHeader = view.Columns.Add("Total Time");
            var callCountHeader = view.Columns.Add("# of Calls");
            var whereHeader = view.Columns.Add("Where");

            foreach (var sql in sqls)
            {
                ListViewItem item = new ListViewItem();
                item.Tag = sql;
                item.Text = sql.TotalTime.ToString();
                item.SubItems.Add(sql.NumberOfCalls.ToString());
                item.SubItems.Add(sql.WhereClause);

                if (sql.HasError)
                {
                    item.BackColor = System.Drawing.Color.Yellow;
                }

                view.Items.Add(item);
            }

            totalHeader.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
            callCountHeader.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
            whereHeader.AutoResize(ColumnHeaderAutoResizeStyle.ColumnContent);
            view.EndUpdate();
        }

        public static void BuildSQLTableList(ListView view, List<SQLStatement> statements)
        {
            view.BeginUpdate();
            view.Columns.Clear();
            view.Items.Clear();

            var typeHeader = view.Columns.Add("SQL Type");
            var tableHeader = view.Columns.Add("Table(s)");
            var countHeader = view.Columns.Add("Count");

            List<SQLStatement> selects = new List<SQLStatement>();
            List<SQLStatement> inserts = new List<SQLStatement>();
            List<SQLStatement> updates = new List<SQLStatement>();
            List<SQLStatement> deletes = new List<SQLStatement>();

            foreach(var statement in statements)
            {
                switch(statement.Type)
                {
                    case SQLType.SELECT:
                        selects.Add(statement);
                        break;
                    case SQLType.INSERT:
                        inserts.Add(statement);
                        break;
                    case SQLType.UPDATE:
                        updates.Add(statement);
                        break;
                    case SQLType.DELETE:
                        deletes.Add(statement);
                        break;
                }
            }

            var selectGroups = selects.GroupBy(s => string.Join(", ", s.Tables.ToArray()));
            foreach (var group in selectGroups)
            {
                ListViewItem item = new ListViewItem();
                item.Tag = group;
                SQLStatement firstSQL = group.First();
                item.Text = firstSQL.Type.ToString();
                item.SubItems.Add(string.Join(", ", firstSQL.Tables.ToArray()));
                item.SubItems.Add(group.Count().ToString());
                view.Items.Add(item);
            }

            var insertGroup = inserts.GroupBy(s => string.Join(", ", s.Tables.ToArray()));
            foreach (var group in insertGroup)
            {
                ListViewItem item = new ListViewItem();
                item.Tag = group;
                SQLStatement firstSQL = group.First();
                item.Text = firstSQL.Type.ToString();
                item.SubItems.Add(string.Join(", ", firstSQL.Tables.ToArray()));
                item.SubItems.Add(group.Count().ToString());
                view.Items.Add(item);
            }

            var updateGroup = updates.GroupBy(s => string.Join(", ", s.Tables.ToArray()));
            foreach (var group in updateGroup)
            {
                ListViewItem item = new ListViewItem();
                item.Tag = group;
                SQLStatement firstSQL = group.First();
                item.Text = firstSQL.Type.ToString();
                item.SubItems.Add(string.Join(", ", firstSQL.Tables.ToArray()));
                item.SubItems.Add(group.Count().ToString());
                view.Items.Add(item);
            }

            var deleteGroup = deletes.GroupBy(s => string.Join(", ", s.Tables.ToArray()));
            foreach (var group in deleteGroup)
            {
                ListViewItem item = new ListViewItem();
                item.Tag = group;
                SQLStatement firstSQL = group.First();
                item.Text = firstSQL.Type.ToString();
                item.SubItems.Add(string.Join(", ", firstSQL.Tables.ToArray()));
                item.SubItems.Add(group.Count().ToString());
                view.Items.Add(item);
            }
            typeHeader.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
            tableHeader.AutoResize(ColumnHeaderAutoResizeStyle.ColumnContent);
            countHeader.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
            view.EndUpdate();
        }

        internal static void BuildPPCObjectList(TraceData traceData, TreeView ppcObjectTree)
        {
            ppcObjectTree.Nodes.Clear();
            TreeNode AppClassRoot = ppcObjectTree.Nodes.Add("Application Classes");
            TreeNode AppEngineRoot = ppcObjectTree.Nodes.Add("Application Engines");
            TreeNode PageRoot = null;
            TreeNode CompRecFieldRoot = null;
            TreeNode CompRecRoot = null;
            TreeNode RecFieldRoot = null;
            TreeNode RecFuncRoot = null;
            Dictionary<string, TreeNode> AENodeRoots = new Dictionary<string, TreeNode>();
            Dictionary<string, TreeNode> ClassNodeRoots = new Dictionary<string, TreeNode>();
            Dictionary<string, TreeNode> PageNodeRoots = new Dictionary<string, TreeNode>();
            Dictionary<string, TreeNode> CompRecNodeRoots = new Dictionary<string, TreeNode>();
            Dictionary<string, TreeNode> CompRecFldNodeRoots = new Dictionary<string, TreeNode>();
            Dictionary<string, TreeNode> RecFldNodeRoots = new Dictionary<string, TreeNode>();
            Dictionary<string, TreeNode> RecFuncNodeRoots = new Dictionary<string, TreeNode>();
            Dictionary<TreeNode, int> NodeCounts = new Dictionary<TreeNode, int>();

            foreach (var x in traceData.AllExecutionCalls)
            {
                /* Record "IScript_StartPage WEBLIB_IS_MOBR.ISCRIPT1.FieldFormula" */
                /* Function "ext: FUNCLIB_PTBR.FUNCLIB FieldFormula fcn=setThemeCookieForPortal #params=1" */
                /* Function "setThemeCookieForPortal FUNCLIB_PTBR.FUNCLIB.FieldFormula" */
                /* App Class Constructor "BrandingManager PTBR_BRANDING.BrandingManager.OnExecute" */
                /* App Class method "writeThemeAssignmentCookieForPortal PTBR_BRANDING.BrandingManager.OnExecute" */
                if (x.Function.EndsWith(".OnExecute"))
                {
                    if (x.Function.Contains((' ')))
                    {
                        var appClassMethodParts = x.Function.Split(' ');
                        var appClassPackage = appClassMethodParts[1];
                        var methodName = appClassMethodParts[0];
                        var packageParts = appClassPackage.Split('.');
                        /* app class */
                        var parentNode = AppClassRoot;
                        /* build out package nodes if needed */
                        /* have we already built out this whole thing? */
                        if (ClassNodeRoots.ContainsKey(x.Function) == false)
                        {

                            for (var y = 0; y < packageParts.Length - 1; y++)
                            {
                                var searchString = String.Join(".", packageParts, 0, y + 1);
                                if (ClassNodeRoots.ContainsKey(searchString) == false)
                                {
                                    var newNode = new TreeNode(packageParts[y]);
                                    parentNode.Nodes.Add(newNode);
                                    ClassNodeRoots.Add(searchString, newNode);
                                    parentNode = newNode;
                                }
                                else
                                {
                                    parentNode = ClassNodeRoots[searchString];
                                }
                            }

                            /* we've built all of the packages, now lets add the actual method node */
                            var method = parentNode.Nodes.Add(methodName);
                            method.Tag = x;
                            ClassNodeRoots.Add(x.Function, method);
                            NodeCounts.Add(method, 1);
                        }
                        else
                        {
                            NodeCounts[ClassNodeRoots[x.Function]] = NodeCounts[ClassNodeRoots[x.Function]] + 1;
                        }

                        /* get method node */
                        var methodNode = ClassNodeRoots[x.Function];

                        if (x.IsError)
                        {
                            methodNode.BackColor = Color.Red;
                            var methodParent = methodNode.Parent;
                            while (methodParent != null)
                            {
                                methodParent.BackColor = Color.Yellow;
                                methodParent = methodParent.Parent;
                            }
                        }
                        else if (x.Duration >= Properties.Settings.Default.LongCall &&
                                 methodNode.BackColor != Color.Yellow)
                        {
                            methodNode.BackColor = Color.LightGreen;
                            var methodParent = methodNode.Parent;
                            while (methodParent != null)
                            {
                                methodParent.BackColor = methodNode.BackColor;
                                methodParent = methodParent.Parent;
                            }
                        }
                    }
                    else if (x.Function.Split('.').Length == 7)
                    {
                        /* App Engine code */
                        if (AENodeRoots.ContainsKey(x.Function.Replace(".OnExecute","")) == false)
                        {
                            var aeParts = x.Function.Split('.');

                            var parentNode = AppEngineRoot;

                            for (var y = 0; y < aeParts.Length - 1; y++)
                            {
                                var searchString = String.Join(".", aeParts, 0, y + 1);
                                if (AENodeRoots.ContainsKey(searchString) == false)
                                {
                                    var newNode = new TreeNode(aeParts[y]);
                                    parentNode.Nodes.Add(newNode);
                                    AENodeRoots.Add(searchString, newNode);
                                    parentNode = newNode;
                                }
                                else
                                {
                                    parentNode = AENodeRoots[searchString];
                                }
                            }
                            NodeCounts.Add(parentNode, 1);
                        }
                        else
                        {
                            /* entry already exists, just increment */
                            NodeCounts[AENodeRoots[x.Function.Replace(".OnExecute","")]] = NodeCounts[AENodeRoots[x.Function.Replace(".OnExecute","")]]++;

                        }

                        var stepNode = AENodeRoots[x.Function.Replace(".OnExecute", "")];
                        if (x.IsError)
                        {
                            stepNode.BackColor = Color.Red;
                            var methodParent = stepNode.Parent;
                            while (methodParent != null)
                            {
                                methodParent.BackColor = Color.Yellow;
                                methodParent = methodParent.Parent;
                            }
                        }
                        else if (x.Duration >= Properties.Settings.Default.LongCall &&
                                 stepNode.BackColor != Color.Yellow)
                        {
                            stepNode.BackColor = Color.LightGreen;
                            var methodParent = stepNode.Parent;
                            while (methodParent != null)
                            {
                                methodParent.BackColor = stepNode.BackColor;
                                methodParent = methodParent.Parent;
                            }
                        }

                    }
                }
                else if (x.Function.EndsWith("Activate"))
                {
                    /* Page activate */
                    if (PageRoot == null)
                    {
                        PageRoot = ppcObjectTree.Nodes.Add("Pages");
                    }
                    if (PageNodeRoots.ContainsKey(x.Function) == false)
                    {
                        var pageNode = PageRoot.Nodes.Add(x.Function.Split('.')[0]);
                        PageNodeRoots.Add(x.Function, pageNode);
                        NodeCounts.Add(pageNode, 1);

                        if (x.IsError)
                        {
                            pageNode.BackColor = Color.Red;
                            var pageParent = pageNode.Parent;
                            while (pageParent != null)
                            {
                                pageParent.BackColor = Color.Yellow;
                                pageParent = pageParent.Parent;
                            }
                        }
                        else if (x.Duration >= Properties.Settings.Default.LongCall && pageNode.BackColor != Color.Yellow)
                        {
                            pageNode.BackColor = Color.LightGreen;
                            var pageParent = pageNode.Parent;
                            while (pageParent != null)
                            {
                                pageParent.BackColor = pageNode.BackColor;
                                pageParent = pageParent.Parent;
                            }
                        }
                    }
                    else
                    {
                        /* node already exists, just increment its count */
                        NodeCounts[PageNodeRoots[x.Function]] = NodeCounts[PageNodeRoots[x.Function]] + 1;
                    }
                }
                else
                {
                    /* rest of the stuff we care about is component/record/field stuff */
                    var fieldEvents = new string[] { "FieldDefault", "FieldFormula", "FieldChange", "FieldEdit", "SaveEdit", "RowInit", "SavePreChange", "SavePostChange", "RowSelect", "RowInsert", "RowDelete", "SearchInit", "SearchSave", "Workflow", "PrePopup" };
                    var componentEvents = new string[] { "PreBuild", "PostBuild", "SavePreChange", "SavePostChange", "Workflow" };
                    var compRecordEvents = new string[] { "RowInit", "RowInsert", "RowDelete", "RowSelect", "SaveEdit", "SavePostChange", "SavePreChange" };
                    /* get the tail end of the function */
                    if (x.Function.Contains("."))
                    {
                        var funcString = x.Function.Trim(new char[] { '.', ' ' });
                        var possibleEvent = funcString.Split('.').Last();
                        if (fieldEvents.Contains(possibleEvent) || componentEvents.Contains(possibleEvent) || compRecordEvents.Contains(possibleEvent))
                        {
                            var funcParts = funcString.Split('.');
                            if (funcParts.Length == 5 || funcParts.Length == 4)
                            {
                                TreeNode parentNode = null;
                                Dictionary<string, TreeNode> rootsDict = null;
                                if (funcParts.Length == 5 )
                                {
                                    if (CompRecFieldRoot == null)
                                    {
                                        CompRecFieldRoot = ppcObjectTree.Nodes.Add("Component Record Field");
                                    }
                                    parentNode = CompRecFieldRoot;
                                    rootsDict = CompRecFldNodeRoots;
                                }
                                if (funcParts.Length == 4)
                                {
                                    if (CompRecRoot == null)
                                    {
                                        CompRecRoot = ppcObjectTree.Nodes.Add("Component Record");
                                    }
                                    parentNode = CompRecRoot;
                                    rootsDict = CompRecNodeRoots;
                                }
                                
                                /* component rec field */
                                if (rootsDict.ContainsKey(x.Function) == false)
                                {
                                    for (var y = 0; y < funcParts.Length; y++)
                                    {
                                        var searchString = String.Join(".", funcParts, 0, y + 1);
                                        if (rootsDict.ContainsKey(searchString) == false)
                                        {
                                            var newNode = new TreeNode(funcParts[y]);
                                            parentNode.Nodes.Add(newNode);
                                            rootsDict.Add(searchString, newNode);
                                            parentNode = newNode;
                                            if (y == funcParts.Length - 1)
                                            {
                                                /* this is the last one, the "event" */
                                                newNode.Tag = x;
                                                NodeCounts.Add(newNode, 1);

                                                if (x.IsError)
                                                {
                                                    newNode.BackColor = Color.Red;
                                                    var nodeParent = newNode.Parent;
                                                    while (nodeParent != null)
                                                    {
                                                        nodeParent.BackColor = Color.Yellow;
                                                        nodeParent = nodeParent.Parent;
                                                    }
                                                }
                                                else if (x.Duration >= Properties.Settings.Default.LongCall && newNode.BackColor != Color.Yellow)
                                                {
                                                    newNode.BackColor = Color.LightGreen;
                                                    var nodeParent = newNode.Parent;
                                                    while (nodeParent != null)
                                                    {
                                                        nodeParent.BackColor = newNode.BackColor;
                                                        nodeParent = nodeParent.Parent;
                                                    }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            parentNode = rootsDict[searchString];
                                        }
                                    }

                                }
                                else
                                {
                                    NodeCounts[rootsDict[x.Function]] = NodeCounts[rootsDict[x.Function]] + 1;
                                }


                            }
                            
                            else if (funcParts.Length == 3)
                            {
                                bool isFunc = false;
                                if (funcParts[0].Contains(" "))
                                {
                                    /* they put the method first... lets put it at the end */
                                    var methodName = funcParts[0].Split(' ')[0];
                                    var recordName = funcParts[0].Split(' ')[1];

                                    var newFuncParts = new string[funcParts.Length + 1];
                                    newFuncParts[0] = recordName;
                                    for (var y = 1; y < funcParts.Length; y++)
                                    {
                                        newFuncParts[y] = funcParts[y];
                                    }
                                    newFuncParts[newFuncParts.Length - 1] = methodName;
                                    funcParts = newFuncParts;
                                    isFunc = true;
                                }
                                TreeNode activeRoot = null;
                                Dictionary<string, TreeNode> activeRoots = null;
                                if (isFunc)
                                {
                                    /* record func */
                                    if (RecFuncRoot == null)
                                    {
                                        RecFuncRoot = ppcObjectTree.Nodes.Add("Record Functions");
                                    }
                                    activeRoot = RecFuncRoot;
                                    activeRoots = RecFuncNodeRoots;
                                }
                                else
                                {
                                    /* record field */
                                    if (RecFieldRoot == null)
                                    {
                                        RecFieldRoot = ppcObjectTree.Nodes.Add("Record Field");
                                    }
                                    activeRoot = RecFieldRoot;
                                    activeRoots = RecFldNodeRoots;
                                }


                                var parentNode = activeRoot;
                                /* component rec field */
                                if (activeRoots.ContainsKey(x.Function) == false)
                                {

                                    for (var y = 0; y < funcParts.Length; y++)
                                    {
                                        var searchString = String.Join(".", funcParts, 0, y + 1);
                                        if (activeRoots.ContainsKey(searchString) == false)
                                        {
                                            var newNode = new TreeNode(funcParts[y]);
                                            parentNode.Nodes.Add(newNode);
                                            if (isFunc && y == funcParts.Length - 1)
                                            {
                                                activeRoots.Add(x.Function, newNode);
                                            }
                                            else
                                            {
                                                activeRoots.Add(searchString, newNode);
                                            }
                                            parentNode = newNode;
                                            if (y == funcParts.Length - 1)
                                            {
                                                /* this is the last one, the "event" */
                                                newNode.Tag = x;
                                                NodeCounts.Add(newNode, 1);

                                                if (x.IsError)
                                                {
                                                    newNode.BackColor = Color.Red;
                                                    var nodeParent = newNode.Parent;
                                                    while (nodeParent != null)
                                                    {
                                                        nodeParent.BackColor = Color.Yellow;
                                                        nodeParent = nodeParent.Parent;
                                                    }
                                                }
                                                else if (x.Duration >= Properties.Settings.Default.LongCall && newNode.BackColor != Color.Yellow)
                                                {
                                                    newNode.BackColor = Color.LightGreen;
                                                    var nodeParent = newNode.Parent;
                                                    while (nodeParent != null)
                                                    {
                                                        nodeParent.BackColor = newNode.BackColor;
                                                        nodeParent = nodeParent.Parent;
                                                    }
                                                }

                                            }
                                        }
                                        else
                                        {
                                            parentNode = activeRoots[searchString];
                                        }
                                    }

                                }
                                else
                                {
                                    NodeCounts[activeRoots[x.Function]] = NodeCounts[activeRoots[x.Function]] + 1;
                                }

                            }
                            else
                            {
                                MessageBox.Show("Encountered an unsupported type of PPC call: " + x.Function + " Please report this on Trace Wizard's github issues.");
                            }
                        }
                    }
                }
            }
            /* update the node counts */
            var nodesToPropagate = NodeCounts.Keys.ToList();

            for (var x = 0; x < nodesToPropagate.Count; x++)
            {
                var node = nodesToPropagate[x];
                if (node.Parent == null)
                {
                    continue;
                }
                if (NodeCounts.ContainsKey(node.Parent))
                {
                    NodeCounts[node.Parent] = NodeCounts[node.Parent] + NodeCounts[node];
                } else
                {
                    NodeCounts.Add(node.Parent, NodeCounts[node]);
                    if (node.Parent != null)
                    {
                        nodesToPropagate.Add(node.Parent);
                    }
                }
            }

            foreach (TreeNode node in NodeCounts.Keys.ToList())
            {
                node.Text += " - " + NodeCounts[node] + (NodeCounts[node] == 1 ? " call" : " calls");
            }
        }

        internal static void BuildVariableList(ListView lstVariables, List<VariableBundle> variables)
        {
            lstVariables.BeginUpdate();
            lstVariables.Columns.Clear();
            lstVariables.Items.Clear();

            var ctxHeader = lstVariables.Columns.Add("Context");
            var typeHeader = lstVariables.Columns.Add("Type");
            var countHeader = lstVariables.Columns.Add("Variable Count");

            lstVariables.Items.Clear();
            foreach(var bundle in variables)
            {
                
                ListViewItem item = new ListViewItem();
                item.Tag = bundle;
                item.Text = bundle.Context.ToString();
                switch (bundle.Type)
                {
                    case BundleType.COMPONENT_SERIALIZED:
                        item.SubItems.Add("Component Serialized");
                        break;
                    case BundleType.COMPONENT_DESERIALIZED:
                        item.SubItems.Add("Component Deserialized");
                        break;
                    case BundleType.GLOBAL_SERIALIZED:
                        item.SubItems.Add("Global Serialized");
                        break;
                    case BundleType.GLOBAL_DESERIALIZED:
                        item.SubItems.Add("Global Deserialized");
                        break;
                }

                item.SubItems.Add(bundle.Variables.Count.ToString());
                lstVariables.Items.Add(item);

            }
            ctxHeader.AutoResize(ColumnHeaderAutoResizeStyle.ColumnContent);
            typeHeader.AutoResize(ColumnHeaderAutoResizeStyle.ColumnContent);
            countHeader.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
            lstVariables.EndUpdate();

        }

        public static void BuildFromSQLList(ListView view, List<SQLByFrom> sqls)
        {
            view.BeginUpdate();
            view.Columns.Clear();
            view.Items.Clear();

            var totalHeader = view.Columns.Add("Total Time");
            var callCountHeader = view.Columns.Add("# of Calls");
            var whereHeader = view.Columns.Add("From");

            foreach (var sql in sqls)
            {
                ListViewItem item = new ListViewItem();
                item.Tag = sql;
                item.Text = sql.TotalTime.ToString();
                item.SubItems.Add(sql.NumberOfCalls.ToString());
                item.SubItems.Add(sql.FromClause);

                if (sql.HasError)
                {
                    item.BackColor = System.Drawing.Color.Yellow;
                }

                view.Items.Add(item);
            }

            totalHeader.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
            callCountHeader.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
            whereHeader.AutoResize(ColumnHeaderAutoResizeStyle.ColumnContent);
            view.EndUpdate();
        }

        public static void BuildStackTraceList(ListView view, List<StackTraceEntry> traces)
        {
            view.BeginUpdate();
            view.Columns.Clear();
            view.Items.Clear();

            view.Columns.Add("Line #");
            view.Columns.Add("Message");
            view.Columns.Add("Offender");

            traces.OrderBy(p => p.LineNumber);

            foreach (var t in traces)
            {
                ListViewItem item = new ListViewItem();
                item.Tag = t;
                item.Text = t.LineNumber.ToString();
                item.SubItems.Add(t.Message);
                item.SubItems.Add(t.Offender);

                view.Items.Add(item);
            }

            foreach (ColumnHeader header in view.Columns)
            {
                if (header.Text == "Line #")
                {
                    header.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
                }
                else
                {
                    header.AutoResize(ColumnHeaderAutoResizeStyle.ColumnContent);
                }

            }
            view.EndUpdate();

        }

        public static void BuildExecutionTree(TraceData traceData, TreeView executionTree, Dictionary<SQLStatement, TreeNode> SQLMapToTree, Dictionary<ExecutionCall, TreeNode> ExecCallToTree, bool showLoading = true, bool diffMode = false)
        {
            
            if (traceData == null)
            {
                return;
            }
            var execList = traceData.ExecutionPath;
            var sqlList = traceData.SQLStatements;

            executionTree.Nodes.Clear();
            SQLMapToTree.Clear();
            ExecCallToTree.Clear();

            var contextList = execList.OrderBy(p => p.StartLine).Select(p => p.Context).Distinct().ToList();
            var contextNodeList = new List<TreeNode>();
            double totalTraceTime = 0;
            foreach (var ctx in contextList)
            {
                var ctxNode = new TreeNode(ctx);
                contextNodeList.Add(ctxNode);
                var rootExecCalls = execList.Where(p => p.Context.Equals(ctx)).OrderBy(p => p.StartLine);
                double contextTotalTime = 0;
                foreach (var exec in rootExecCalls)
                {
                    contextTotalTime += exec.Duration;
                    if (!diffMode)
                    {
                        if (exec.HasError || exec.IsError)
                        {
                            ctxNode.BackColor = Color.Yellow;
                        }
                        else if (exec.Duration >= Properties.Settings.Default.LongCall)
                        {
                            ctxNode.BackColor = Color.LightGreen;
                        }
                    } else
                    {
                        /* only color yellow in Diff mode if there was a MODIFIED */
                if (exec.DiffStatus == DiffStatus.MODIFIED)
                        {
                            ctxNode.BackColor = Color.Yellow;
                        }
                    }
                    UIBuilder.BuildExecutionTree(ctxNode, exec,SQLMapToTree, ExecCallToTree, showLoading,diffMode);
                }
                if (rootExecCalls.First().Type.HasFlag(ExecutionCallType.AE) == false)
                {
                    ctxNode.Text += " Dur: " + contextTotalTime;
                }
                totalTraceTime += contextTotalTime;
            }
            foreach (var node in contextNodeList)
            {
                executionTree.Nodes.Add(node);
            }
        }

        public static void BuildExecutionTree(TreeNode root, ExecutionCall call, Dictionary<SQLStatement, TreeNode> SQLMapToTree, Dictionary<ExecutionCall, TreeNode> ExecCallToTree, bool showLoading, bool diffMode = false)
        {
            TreeNode newRoot = null;
            if (call.Type == ExecutionCallType.SQL)
            {
                var sqlItem = call.SQLStatement;
                switch (sqlItem.Type)
                {
                    case SQLType.SELECT:
                        newRoot = root.Nodes.Add("SELECT FROM " + sqlItem.FromClause + "Fetched=" + sqlItem.FetchCount + " Dur=" + sqlItem.Duration);
                        break;
                    case SQLType.UPDATE:
                        newRoot = root.Nodes.Add("UPDATE " + sqlItem.FromClause + " Dur=" + sqlItem.Duration);
                        break;
                    case SQLType.INSERT:
                        newRoot = root.Nodes.Add("INSERT INTO " + sqlItem.FromClause + " Dur=" + sqlItem.Duration);
                        break;
                    case SQLType.DELETE:
                        newRoot = root.Nodes.Add("DELETE FROM " + sqlItem.FromClause + " Dur=" + sqlItem.Duration);
                        break;
                }

                SQLMapToTree.Add(sqlItem, newRoot);
                newRoot.Tag = call;
                if (!diffMode)
                {
                    if (sqlItem.IsError)
                    {
                        newRoot.BackColor = Color.Red;
                    }
                } else
                {
                    if (call.DiffStatus == DiffStatus.DELETE)
                    {
                        newRoot.BackColor = Color.Red;
                    }
                    else if (call.DiffStatus == DiffStatus.INSERT)
                    {
                        newRoot.BackColor = Color.LightBlue;
                    } else if (call.DiffStatus == DiffStatus.MODIFIED) 
                    {
                        newRoot.BackColor = Color.Yellow;
                    }
                }
                
            }
            else
            {
                newRoot = root.Nodes.Add(call.Function + (call.Type.HasFlag(ExecutionCallType.AE) ? "" : "  Dur: " + (call.Duration)));
                ExecCallToTree.Add(call, newRoot);

                if (call.Type.HasFlag(ExecutionCallType.SQL) && call.SQLStatement != null)
                {
                    SQLMapToTree.Add(call.SQLStatement, newRoot);
                    newRoot.Tag = call;
                }

                if (!diffMode)
                {
                    if (call.HasError)
                    {
                        newRoot.BackColor = Color.Yellow;
                    }
                    else if (call.IsError)
                    {
                        newRoot.BackColor = Color.Red;
                    }
                    else if (call.Duration >= Properties.Settings.Default.LongCall)
                    {
                        newRoot.BackColor = Color.LightGreen;
                    }
                } else
                {
                    if (call.DiffStatus == DiffStatus.INSERT)
                    {
                        newRoot.BackColor = Color.LightBlue;
                    }
                    else if (call.DiffStatus == DiffStatus.DELETE)
                    {
                        newRoot.BackColor = Color.Red;
                    }
                    else if (call.DiffStatus == DiffStatus.MODIFIED)
                    {
                        newRoot.BackColor = Color.Yellow;
                    }
                }
                
                newRoot.Tag = call;
                if (showLoading && call.Children.Count > 0)
                {
                    newRoot.Nodes.Add("Loading...");
                }
            }

        }

    }
}
