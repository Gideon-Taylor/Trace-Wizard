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
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace TraceWizard.Data
{
    [Serializable]
    public class SQLExecution
    {
        public List<SQLBindValue> BindValues = new List<SQLBindValue>();
        public double ExecTime;
        public double FetchTime;

        public int FetchCount;
        public bool BindsOpen = true;
    }

    [Serializable]
    public class SQLStatement
    {
        public static uint NextID;

        public uint InternalID = NextID++;

        SQLExecution currentExecution;

        public SQLStatement()
        {

        }

        public long LineNumber;
        public double Duration
        {
            get
            {
                return ExecTime + FetchTime;
            }
        }

        public bool IsSelectInit
        {
            get
            {
                return Statement.StartsWith("%SelectInit");
            }
        }

        public ExecutionCall ParentCall;

        public string SQLID;
        public string Statement;
        public int Cursor;
        public string WhereClause;
        public string FromClause;
        public double ExecTime {
            get {
                return currentExecution.ExecTime;
            }
            set {
                currentExecution.BindsOpen = false;
                currentExecution.ExecTime = value;
            }
        }
        public double FetchTime {
            get
            {
                return currentExecution.FetchTime;
            }
            set
            {

                currentExecution.FetchTime = value;
            }
        }

        public int FetchCount
        {
            get
            {
                return currentExecution.FetchCount;
            }
            set
            {
                currentExecution.FetchCount = value;
            }
        }

        public int TotalExecutions
        {
            get
            {
                return Executions.Count;
            }
        }

        public double TotalExecTime
        {
            get
            {
                return Executions.Sum(e => e.ExecTime);
            }
        }

        public double TotalFetchTime
        {
            get
            {
                return Executions.Sum(e => e.FetchTime);
            }
        }

        public double AverageExecTime
        {
            get
            {
                return ExecTime / TotalExecutions;
            }
        }

        public double AverageFetchTime
        {
            get
            {
                return FetchTime / TotalExecutions;
            }
        }

        public double AverageDuration
        {
            get
            {
                return Duration / TotalExecutions;
            }
        }

        public SQLExecution CurrentExecution
        {
            get
            {
                return currentExecution;
            }
        }

        public bool IsError;
        public bool Cobol;
        public SQLError ErrorInfo;
        
        public int RCNumber;
        public List<SQLExecution> Executions = new List<SQLExecution>();
        public List<string> BufferData = null;
        public List<String> Tables = new List<string>();
        public SQLType Type;

        
        public string Context;

        public SQLStatement(string text)
        {
            Statement = text.Trim();
            currentExecution = new SQLExecution();
            AddExecution();
            DetermineType();
            ParseWhereClause();
            ParseFromClause();
            GenerateSQLID();
        }

        public void AddBindValue(SQLBindValue bind)
        {
            if (currentExecution.BindsOpen == false)
            {
                currentExecution = new SQLExecution();
                Executions.Add(currentExecution);
            }
            currentExecution.BindValues.Add(bind);
        }

        private void AddExecution()
        {
            currentExecution = new SQLExecution();
            Executions.Add(currentExecution);
        }
        private void GenerateSQLID()
        {
            MD5CryptoServiceProvider hashlib = new MD5CryptoServiceProvider();
            byte[] arrData = null;
            byte[] byteHash = null;
            string sqlid = "";
            const string alphabet = "0123456789abcdfghjkmnpqrstuvwxyz";
            UInt64 MSB = default(UInt64);
            UInt64 LSB = default(UInt64);
            UInt64 sqln = default(UInt64);
            UInt32[] arr3 = {0,0,0,0};
            UInt32[] arr4 = { 0, 0, 0, 0 };
            string sql_text = Statement;

            sql_text = (sql_text + "\0");
            arrData = System.Text.Encoding.ASCII.GetBytes(sql_text);
            byteHash = hashlib.ComputeHash(arrData);
            Buffer.BlockCopy(byteHash, 8, arr3, 0, 4);
            Buffer.BlockCopy(byteHash, 12, arr4, 0, 4);
            MSB = (((arr3[0] | (arr3[1] << 8)) | (arr3[2] << 0x10)) | (arr3[3] << 0x18));
            LSB = (((arr4[0] | (arr4[1] << 8)) | (arr4[2] << 0x10)) | (arr4[3] << 0x18));
            sqln = (MSB << 32) + LSB;
            for (int iCount = 0; iCount <= 12; iCount++)
            {
                sqlid = alphabet[Convert.ToInt32((sqln >> (iCount * 5)) % 32)] + sqlid;
            }
            SQLID = sqlid;
        }
        private void ParseWhereClause()
        {
            Regex whereClause = new Regex(" WHERE (.*?)(ORDER|$)",RegexOptions.IgnoreCase);
            Match m = whereClause.Match(Statement);
            if (m.Success)
            {
                WhereClause = m.Groups[1].Value.Trim();
            } else
            {
                WhereClause = "";
            }
             
        }

        private void ParseFromClause()
        {
            Regex fromRegex = null;
            switch(Type)
            {
                case SQLType.SELECT:
                    fromRegex = new Regex("\\s+FROM\\s*(.*?)\\s*(WHERE|$)", RegexOptions.IgnoreCase);
                    break;
                case SQLType.UPDATE:
                    fromRegex = new Regex("UPDATE\\s*(.*?)\\s*(SET|$)", RegexOptions.IgnoreCase);
                    break;
                case SQLType.INSERT:
                    fromRegex = new Regex("INTO\\s*(.*?)\\s*(VALUES|\\(|$)", RegexOptions.IgnoreCase);
                    break;
                case SQLType.DELETE:
                    fromRegex = new Regex("DELETE FROM\\s*(.*?)\\s*(WHERE|$)", RegexOptions.IgnoreCase);
                    break;
            }

            FromClause = "";
            foreach (Match match in fromRegex.Matches(Statement))
            {
                FromClause += match.Groups[1].Value.Trim() + " ";
            }

            //FromClause = fromRegex.Match(Statement).Groups[1].Value.Trim();

            /* determine tables in the clause */
            if (Type == SQLType.SELECT)
            {
                var parts = FromClause.Split(',');
                foreach (var part in parts)
                {
                    var words = part.Trim().Split(' ');
                    Tables.Add(words[0]);
                }
            } else
            {
                Tables.Add(FromClause);
            }
        }
        internal Dictionary<string,string> GetBufferItems()
        {
            Dictionary<string, string> items = new Dictionary<string, string>();
            List<string> columns = GetBufferColumns();
            for(var x = 0; x < columns.Count; x++)
            {
                items.Add(columns[x], BufferData[x]);
            }
            return items;
        }
        internal List<string> GetBufferColumns()
        {
            var columns = new List<string>();

            /* is this a %Select type query? */
            if (Statement.StartsWith("%Select",StringComparison.InvariantCultureIgnoreCase))
            {
                if (BufferData != null && BufferData.Count > 0)
                {
                    Regex getBufferColumns = new Regex(@"%Select(?:Init)?\((.*?)\)");
                    var bufferCols = getBufferColumns.Match(Statement).Groups[1].Value;

                    Regex colSplit = new Regex(@"([^, ]+)");
                    foreach (Match match in colSplit.Matches(bufferCols))
                    {
                        columns.Add(match.Groups[1].Value);
                    }
                }
            }


            return columns;
        }

        private void DetermineType()
        {
            if (Statement.StartsWith("SELECT", StringComparison.InvariantCultureIgnoreCase))
            {
                this.Type = SQLType.SELECT;
            }
            if (Statement.StartsWith("UPDATE", StringComparison.InvariantCultureIgnoreCase))
            {
                this.Type = SQLType.UPDATE;
            }
            if (Statement.StartsWith("DELETE", StringComparison.InvariantCultureIgnoreCase))
            {
                this.Type = SQLType.DELETE;
            }
            if (Statement.StartsWith("INSERT", StringComparison.InvariantCultureIgnoreCase))
            {
                this.Type = SQLType.INSERT;
            }
        }

        public override string ToString()
        {
            return Statement;
        }
    }

    [Serializable]
    public class SQLBindValue
    {
        public static uint NextID;

        public uint InternalID = NextID++;
        public int Index;
        public int Type;
        public string TypeString;
        public int Length;
        public string Value;
    }

    [Serializable]
    public enum SQLType
    {
        SELECT,UPDATE,DELETE,INSERT
    }

    [Serializable]
    public class SQLError
    {
        public static uint NextID;

        public uint InternalID = NextID++;
        public int ErrorPosition;
        public int ReturnCode;
        public string Message;

    }
}
