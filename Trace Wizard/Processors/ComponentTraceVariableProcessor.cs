using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    class ComponentTraceVariableProcessor : ITraceProcessor
    {
        bool ParsingVariables = false;

        Regex contextMarker = new Regex("PSAPPSRV\\.\\d+ \\(\\d+\\)");
        Regex startMarker = new Regex("Begin PeopleCode (Component|Global) Variables: (Serialized|Deserialized) Buffers");
        Regex endMarker = new Regex("End PeopleCode (Component|Global) Variables: (Serialized|Deserialized) Buffers");
        Regex variableLine = new Regex(@"\d\.\d{6}(\s+)(.*?)\s*:\s*(.*)");

        List<VariableBundle> Bundles = new List<VariableBundle>();
        VariableBundle currentBundle = null;
        Stack<Variable> varStack = new Stack<Variable>();
        public void ProcessLine(string line, long lineNumber)
        {
            if (ParsingVariables == false)
            {
                /* check for a Begin block */
                var match = startMarker.Match(line);
                if (match.Success)
                {
                    currentBundle = new VariableBundle();
                    var currentContextString = contextMarker.Match(line).Groups[0].Value;
                    currentBundle.StartLine = lineNumber;
                    currentBundle.Context = currentContextString;

                    switch (match.Groups[1].Value)
                    {
                        case "Component":
                            switch (match.Groups[2].Value)
                            {
                                case "Serialized":
                                    currentBundle.Type = BundleType.COMPONENT_SERIALIZED;
                                    break;
                                case "Deserialized":
                                    currentBundle.Type = BundleType.COMPONENT_DESERIALIZED;
                                    break;
                            }
                            break;
                        case "Global":
                            switch (match.Groups[2].Value)
                            {
                                case "Serialized":
                                    currentBundle.Type = BundleType.GLOBAL_SERIALIZED;
                                    break;
                                case "Deserialized":
                                    currentBundle.Type = BundleType.GLOBAL_DESERIALIZED;
                                    break;
                            }
                            break;
                    }
                    currentBundle.Variables = new List<Variable>();
                    Bundles.Add(currentBundle);
                    ParsingVariables = true;
                    return;
                }
                else
                {
                    return;
                }
            }

            if (endMarker.IsMatch(line))
            {
                /* we've reached the end */
                while(varStack.Count > 0)
                {
                    currentBundle.Variables.Add(varStack.Pop());
                }
                currentBundle = null;
                ParsingVariables = false;
            }
            else
            {
                /* We are processing variables. extract the info we need */
                var match = variableLine.Match(line);
                int indent = match.Groups[1].Value.Length;
                string leftSide = match.Groups[2].Value.Trim();
                string rightSide = match.Groups[3].Value.Trim();


                while (varStack.Count > 0 && varStack.Peek().Depth >= indent)
                {
                    var popped = varStack.Pop();
                    if (popped.Parent == null)
                    {
                        currentBundle.Variables.Add(popped);
                    }
                }
                var newVar = CreateVar(leftSide, rightSide, indent);

                if (varStack.Count > 0)
                {
                    /* there's still an object above us, add us to that */

                    /* check for array metadata - ignore if so */
                    if (leftSide == "Dimension" || leftSide == "Len" && varStack.Peek().IsArray)
                    {
                        return;
                    }
                    newVar.Parent = varStack.Peek();
                    varStack.Peek().ChildValues.Add(newVar);
                    varStack.Push(newVar);

                }
                else
                {
                    varStack.Push(newVar);
                }
            }

            //throw new NotImplementedException();
        }

        Variable CreateVar(string leftSide, string rightSide, int indent)
        {
            var curVar = new Variable();
            curVar.Name = leftSide;
            curVar.Type = rightSide;
            curVar.Depth = indent;
            if (rightSide.Contains(":") && rightSide.StartsWith("Str") == false)
            {
                curVar.IsObject = true;
                curVar.ChildValues = new List<Variable>();
            }
            if (rightSide.Equals("Array"))
            {
                curVar.IsArray = true;
                curVar.ChildValues = new List<Variable>();
            }
            if (rightSide.StartsWith("Array") && rightSide.Contains(" of "))
            {
                curVar.IsArray = true;
                curVar.ChildValues = new List<Variable>();
                curVar.Type = rightSide.Split(new string[] { " of " }, StringSplitOptions.RemoveEmptyEntries)[1];
            }
            return curVar;
        }

        public void ProcessorComplete(TraceData data)
        {
            data.Variables = Bundles;
            //throw new NotImplementedException();
        }

        public void ProcessorInit(TraceData data)
        {
            //throw new NotImplementedException();
        }
    }
}
