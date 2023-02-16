using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CM3D2.Toolkit.Guest4168Branch.Arc.Entry;
using CM3D2.Toolkit.Guest4168Branch.MultiArcLoader;
using CM3D2.Toolkit.Guest4168Branch.Logging;

namespace CM3D2.MultiArc.Example
{
    class Program
    {
        static void Main(string[] args)
        {

            //ScriptPatternBetter(ScriptTag.Face);
            //ScriptPatternBetter(ScriptTag.MotionScript);
            //ScriptMotionScriptAnalysis2();
            //ScriptCompleteAnalysis();
            //ScriptPatternBetter(ScriptTag.PlaySE);

            IsExist();

            Console.ReadLine();
        }

        static void SampleAnm()
        {
            string[] dirs1 = new string[] { @"C:\KISS\CM3D2\GameData" };
            string[] dirs2 = new string[] { @"C:\KISS\COM3D2\GameData_20" };
            string[] dirs3 = new string[] { @"C:\KISS\COM3D2\GameData" };
            string[] dirs4 = new string[] { @"C:\KISS\COM3D2\Mod"};

            //Time
            Stopwatch stopWatch = Stopwatch.StartNew();

            //Load Arcs
            List<List<string>> data = new List<List<string>>();
            MultiArcLoader sal = new MultiArcLoader(dirs1, dirs2, dirs3, dirs4, 5, MultiArcLoader.LoadMethod.Single, false, null, true, MultiArcLoader.Exclude.None | MultiArcLoader.Exclude.BG, new ConsoleLogger());
            sal.LoadArcs();

            //Get only the anm files
            string[] files = sal.GetFileListAtExtension(".nei");
            foreach (string file in files)
            {
                string id = "X".PadLeft(10, '0');
                string name = file.Split(new string[] { ".nei" }, System.StringSplitOptions.RemoveEmptyEntries)[0];
                data.Add(new List<string>() { id, name });
            }

            //Time
            stopWatch.Stop();

            //Base Folders???
            List<ArcDirectoryEntry> level0 = sal.arc.Directories.Where(dir => dir.Depth == 1).ToList();
            foreach (ArcFileEntry file in sal.arc.Files.Values)
            {
                if (!(sal.GetContentsArcFilePath(file) == null || sal.GetContentsArcFilePath(file).Trim().Equals("")))
                {
                    //Console.WriteLine(sal.GetContentsArcFilePath(file));
                }
                else
                {
                    Console.WriteLine("WHERE IS MY MOMMY " + file.FullName);
                }
            }
            Console.WriteLine("ANM Files:" + data.Count + " Time:" + (stopWatch.ElapsedMilliseconds) + " Total Files:" + sal.arc.Files.Count);
        }

        static void ScriptDump()
        {
            string[] dirs1 = new string[] { @"C:\KISS\CM3D2\GameData" };
            string[] dirs2 = new string[] { @"C:\KISS\COM3D2\GameData_20" };
            string[] dirs3 = new string[] { @"C:\KISS\COM3D2\GameData" };
            string[] dirs4 = new string[] { @"C:\KISS\COM3D2\Mod" };

            Stopwatch stopWatch = Stopwatch.StartNew();

            MultiArcLoader sal = new MultiArcLoader(null, dirs2, dirs3, null, 3, MultiArcLoader.LoadMethod.Single, false, null, true, MultiArcLoader.Exclude.None | MultiArcLoader.Exclude.BG | MultiArcLoader.Exclude.CSV | 
                                                                                                                      MultiArcLoader.Exclude.Motion | MultiArcLoader.Exclude.Parts | MultiArcLoader.Exclude.PriorityMaterial | 
                                                                                                                      MultiArcLoader.Exclude.Sound | MultiArcLoader.Exclude.System | MultiArcLoader.Exclude.Voice);
            sal.LoadArcs();

            //Get only the ks files
            //string[] files = sal.GetFileListAtExtension(".ks");
            //foreach (string file in files)
            //{
            //    sal.arc.
            //    Console.WriteLine(file);
            //}
            int count = 0;
            foreach (ArcFileEntry arcFile in sal.arc.Files.Values)
            {
                if (arcFile.Name.EndsWith("." + "ks"))
                {
                    if (!(sal.GetContentsArcFilePath(arcFile) == null || sal.GetContentsArcFilePath(arcFile).Trim().Equals("")))
                    {
                        string path = arcFile.FullName.Replace(@"CM3D2ToolKit:\\root", @"D:\2021-11-07\scripts");
                        if (!path.Contains("cbl"))
                        {
                            //if (!Directory.Exists(Path.GetDirectoryName(path)))
                            //{
                            //    Directory.CreateDirectory(Path.GetDirectoryName(path));
                            //}
                            if (!File.Exists(path))
                            {
                                Console.WriteLine(path);
                                count++;
                                //File.WriteAllText(path, sal.GetContentsArcFilePath(arcFile));
                            }
                            
                        }
                    }
                    else
                    {
                        Console.WriteLine("WHERE IS MY MOMMY " + arcFile.FullName);
                    }

                    //string path = Path.Combine(arcFile.)
                    //if(! File.Exists())
                    //using (FileStream fs = File.Open())
                    //{ 
                    //}
                }
            }
            stopWatch.Stop();
            Console.WriteLine("Time:" + (stopWatch.ElapsedMilliseconds) + " Total Files:" + sal.arc.Files.Count + " Count:" + count); //+ " KS Files:" + files.Length);

            //using (var fs = System.IO.File.Open(@"D:\2021-11-07\scripts.arc", System.IO.FileMode.Create))
            //{
            //    sal.arc.Save(fs);
            //}
        }

        static void ScriptPattern()
        {
            string[] dirs1 = new string[] { @"C:\KISS\CM3D2\GameData" };
            string[] dirs2 = new string[] { @"C:\KISS\COM3D2\GameData_20" };
            string[] dirs3 = new string[] { @"C:\KISS\COM3D2\GameData" };
            string[] dirs4 = new string[] { @"C:\KISS\COM3D2\Mod" };

            MultiArcLoader sal = new MultiArcLoader(null, dirs2, dirs3, null, 3, MultiArcLoader.LoadMethod.Single, false, null, true, MultiArcLoader.Exclude.None | MultiArcLoader.Exclude.BG | MultiArcLoader.Exclude.CSV |
                                                                                                                      MultiArcLoader.Exclude.Motion | MultiArcLoader.Exclude.Parts | MultiArcLoader.Exclude.PriorityMaterial |
                                                                                                                      MultiArcLoader.Exclude.Sound | MultiArcLoader.Exclude.System | MultiArcLoader.Exclude.Voice);
            sal.LoadArcs();

            string tag = @"@face ";
            string param = "name";
            Dictionary<string, Dictionary<string, string>> tagDict = new Dictionary<string, Dictionary<string, string>>();
            List<string> instances = new List<string>();
            List<string> instances2 = new List<string>();

            foreach (ArcFileEntry arcFile in sal.arc.Files.Values)
            {
                if (arcFile.Name.EndsWith("." + "ks"))
                {
                    if (!(sal.GetContentsArcFilePath(arcFile) == null || sal.GetContentsArcFilePath(arcFile).Trim().Equals("")))
                    {
                        string path = arcFile.FullName;
                        if (!path.Contains("cbl"))
                        {
                            arcFile.Pointer = arcFile.Pointer.Decompress();
                            string script = NUty.SjisToUnicode(arcFile.Pointer.Data); //Encoding.UTF8.GetString(arcFile.Pointer.Data);

                            string[] lines = script.Split(new string[] { "\r\n" }, System.StringSplitOptions.None);
                            for (int i = 0; i < lines.Length; i++)
                            {
                                string line = lines[i];
                                if (line.StartsWith(tag))
                                {
                                    string[] tagData = line.Substring(tag.Length).Split(' ');
                                    for(int j=0; j<tagData.Length; j++)
                                    {
                                        string paramsLine = tagData[j];
                                        if (paramsLine.Contains('='))
                                        {
                                            while(paramsLine.Contains(@"= "))
                                            {
                                                paramsLine = paramsLine.Replace(@"= ", @"=");
                                            }
                                            string[] params_ = paramsLine.ToLower().Trim().Split('=');
                                            string paramName = params_[0].Trim().ToLower();
                                            string paramVal = params_[1].Trim().ToLower();
                                            if (paramName.Equals(param) && !instances.Contains(paramVal))
                                            {
                                                instances.Add(paramVal);
                                                instances2.Add(line);
                                            }
                                        }
                                    }
                                    //tagDict[line] = new Dictionary<string, string>();
                                }
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("WHERE IS MY MOMMY " + arcFile.FullName);
                    }

                    //string path = Path.Combine(arcFile.)
                    //if(! File.Exists())
                    //using (FileStream fs = File.Open())
                    //{ 
                    //}
                }
            }

            Console.WriteLine("DONE");
        }

        static void ScriptPatternBetter(ScriptTag tag)
        {
            string[] dirs1 = new string[] { @"C:\KISS\CM3D2\GameData" };
            string[] dirs2 = new string[] { @"C:\KISS\COM3D2\GameData_20" };
            string[] dirs3 = new string[] { @"C:\KISS\COM3D2\GameData" };
            string[] dirs4 = new string[] { @"C:\KISS\COM3D2\Mod" };

            Stopwatch stopwatch;
            stopwatch = Stopwatch.StartNew();
            Console.WriteLine("START: Building Arcs...");
            MultiArcLoader sal = new MultiArcLoader(null, dirs2, dirs3, null, 3, MultiArcLoader.LoadMethod.Single, false, null, true, MultiArcLoader.Exclude.None | MultiArcLoader.Exclude.BG | MultiArcLoader.Exclude.CSV |
                                                                                                                      MultiArcLoader.Exclude.Motion | MultiArcLoader.Exclude.Parts | MultiArcLoader.Exclude.PriorityMaterial |
                                                                                                                      MultiArcLoader.Exclude.Sound | MultiArcLoader.Exclude.System | MultiArcLoader.Exclude.Voice);
            sal.LoadArcs();
            stopwatch.Stop();
            Console.WriteLine("DONE: Building Arcs..." + stopwatch.Elapsed.ToString());

            Console.WriteLine("START: Collecting Tags...");
            stopwatch = Stopwatch.StartNew();
            foreach (ArcFileEntry arcFile in sal.arc.Files.Values)
            {
                if (arcFile.Name.EndsWith("." + "ks"))
                {
                    if (!(sal.GetContentsArcFilePath(arcFile) == null || sal.GetContentsArcFilePath(arcFile).Trim().Equals("")))
                    {
                        string path = arcFile.FullName;
                        if (!path.Contains("cbl"))
                        {
                            arcFile.Pointer = arcFile.Pointer.Decompress();
                            string script = NUty.SjisToUnicode(arcFile.Pointer.Data); //Encoding.UTF8.GetString(arcFile.Pointer.Data);

                            string[] lines = script.Split(new string[] { "\r\n" }, System.StringSplitOptions.None);
                            for (int i = 0; i < lines.Length; i++)
                            {
                                string line = lines[i].TrimStart().ToLower();
                                if (line.StartsWith(tag.tag.TrimStart().ToLower(), StringComparison.OrdinalIgnoreCase))
                                {
                                    //Remove initial tag
                                    line = line.Substring(tag.tag.Length).Trim();
                                    string lineOriginal = line;

                                    //Replace all param names with alternative to fix missing spaces
                                    for (int j = 0; j < tag.paramNames.Count; j++)
                                    {
                                        line = line.Replace(tag.paramNames[j], "|" + tag.paramNames[j]);
                                    }

                                    List<string> paramsSplit = line.Split(new char[]{ '|' }, StringSplitOptions.RemoveEmptyEntries).ToList();
                                    for (int j = 0; j < paramsSplit.Count; j++)
                                    {
                                        //Cleanup whitespace
                                        paramsSplit[j] = paramsSplit[j].Trim();

                                        //Collect
                                        if (paramsSplit[j].Contains("="))
                                        {
                                            string[] paramDetails = paramsSplit[j].Split('=');

                                            if (paramDetails.Length == 2)
                                            {
                                                string paramName = paramDetails[0].Trim().ToLower();
                                                string paramValue = paramDetails[1].Trim().ToLower();

                                                if (tag.paramNames.Contains(paramName + "="))
                                                {
                                                    //If param needs dictionary of value->source
                                                    if (!tag.results.ContainsKey(paramName))
                                                    {
                                                        tag.results[paramName] = new Dictionary<string, string[]>();
                                                    }

                                                    //Special Combo Cases
                                                    if (tag.tag.Equals(ScriptTag.MotionScript.tag) && paramName.Equals("file"))
                                                    {
                                                        string extraParamName = "label";
                                                        for(int k=0; k<paramsSplit.Count; k++)
                                                        {
                                                            string temp = paramsSplit[k].Trim();
                                                            if (temp.Contains("="))
                                                            {
                                                                string[] paramDetails2 = temp.Split('=');

                                                                if (paramDetails2.Length == 2)
                                                                {
                                                                    string paramName2 = paramDetails2[0].Trim().ToLower();
                                                                    string paramValue2 = paramDetails2[1].Trim().ToLower();

                                                                    if (tag.paramNames.Contains(paramName2 + "=") && paramName2.Equals(extraParamName))
                                                                    {
                                                                        paramValue += "~" + paramValue2;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        
                                                    }

                                                    //If param does not have value yet
                                                    if (tag.results.ContainsKey(paramName) && !tag.results[paramName].ContainsKey(paramValue))
                                                    {
                                                        tag.results[paramName][paramValue] = new string[] { lineOriginal, path };
                                                    }
                                                }
                                                else
                                                {
                                                    tag.unknownParams.Add(new List<string>() { paramName, lineOriginal, path });
                                                }
                                            }
                                            else
                                            {
                                                tag.badScripts.Add(new List<string>() { lineOriginal, path });
                                            }
                                        }
                                        else
                                        {
                                            string paramName = paramsSplit[j];
                                            string paramValue = paramName;

                                            //If param needs dictionary of value->source
                                            if (!tag.results.ContainsKey(paramName))
                                            {
                                                if (tag.paramNames.Contains(paramName))
                                                {
                                                    tag.results[paramName] = new Dictionary<string, string[]>();
                                                }
                                                else
                                                {
                                                    tag.unknownParams.Add(new List<string>() { paramName, lineOriginal, path });
                                                }
                                            }

                                            //If param does not have value yet
                                            if (tag.results.ContainsKey(paramName) && !tag.results[paramName].ContainsKey(paramValue))
                                            {
                                                tag.results[paramName][paramValue] = new string[] { lineOriginal, path };
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("LOST ARC: " + arcFile.FullName);
                    }
                }
            }
            stopwatch.Stop();
            Console.WriteLine("DONE: Collecting Tags..." + stopwatch.Elapsed.ToString());

            Console.WriteLine("START: Data to CSV...");
            stopwatch = Stopwatch.StartNew();
            string csv = tag.buildCSV();
            stopwatch.Stop();
            Console.WriteLine("DONE: Data to CSV..." + stopwatch.Elapsed.ToString());
            Console.WriteLine("");
            Console.WriteLine("");
            //Console.WriteLine(csv);
            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine("Press any key to exit");
        }

        static void ScriptMotionScriptAnalysis()
        {
            string[] dirs1 = new string[] { @"C:\KISS\CM3D2\GameData" };
            string[] dirs2 = new string[] { @"C:\KISS\COM3D2\GameData_20" };
            string[] dirs3 = new string[] { @"C:\KISS\COM3D2\GameData" };
            string[] dirs4 = new string[] { @"C:\KISS\COM3D2\Mod" };

            Stopwatch stopwatch;
            stopwatch = Stopwatch.StartNew();
            Console.WriteLine("START: Building Arcs...");
            MultiArcLoader sal = new MultiArcLoader(null, dirs2, dirs3, null, 3, MultiArcLoader.LoadMethod.Single, false, null, true, MultiArcLoader.Exclude.None | MultiArcLoader.Exclude.BG | MultiArcLoader.Exclude.CSV |
                                                                                                                      MultiArcLoader.Exclude.Motion | MultiArcLoader.Exclude.Parts | MultiArcLoader.Exclude.PriorityMaterial |
                                                                                                                      MultiArcLoader.Exclude.Sound | MultiArcLoader.Exclude.System | MultiArcLoader.Exclude.Voice);
            sal.LoadArcs();
            stopwatch.Stop();
            Console.WriteLine("DONE: Building Arcs..." + stopwatch.Elapsed.ToString());

            Console.WriteLine("START: Collecting Labels...");
            stopwatch = Stopwatch.StartNew();

            Dictionary<string, List<string>> data = new Dictionary<string, List<string>>();
            foreach (ArcFileEntry arcFile in sal.arc.Files.Values)
            {
                if (arcFile.Name.EndsWith("." + "ks"))
                {
                    if (!(sal.GetContentsArcFilePath(arcFile) == null || sal.GetContentsArcFilePath(arcFile).Trim().Equals("")))
                    {
                        string path = arcFile.FullName;
                        if (!path.Contains("cbl") && path.Contains(@"\motion\"))
                        {
                            arcFile.Pointer = arcFile.Pointer.Decompress();
                            string script = NUty.SjisToUnicode(arcFile.Pointer.Data); //Encoding.UTF8.GetString(arcFile.Pointer.Data);

                            string[] lines = script.Split(new string[] { "\r\n" }, System.StringSplitOptions.None);
                            for (int i = 0; i < lines.Length; i++)
                            {
                                string line = lines[i].TrimStart().ToLower();
                                if (line.StartsWith(@"*", StringComparison.OrdinalIgnoreCase))
                                {
                                    if(!data.ContainsKey(path))
                                    {
                                        data[path] = new List<string>();
                                    }

                                    if(!data[path].Contains(line))
                                    {
                                        data[path].Add(line);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            stopwatch.Stop();
            Console.WriteLine("DONE: Collecting Labels..." + stopwatch.Elapsed.ToString());

            string csv = "file|label";
            for(int i=0; i<data.Keys.Count; i++)
            {
                for(int j=0; j<data[data.Keys.ToList()[i]].Count; j++)
                {
                    csv += "\n" + data.Keys.ToList()[i] + "|" + data[data.Keys.ToList()[i]][j];
                }
            }

            Console.WriteLine("Press any key to exit");
        }

        static void ScriptMotionScriptAnalysis2()
        {
            string[] dirs1 = new string[] { @"C:\KISS\CM3D2\GameData" };
            string[] dirs2 = new string[] { @"C:\KISS\COM3D2\GameData_20" };
            string[] dirs3 = new string[] { @"C:\KISS\COM3D2\GameData" };
            string[] dirs4 = new string[] { @"C:\KISS\COM3D2\Mod" };

            Stopwatch stopwatch;
            stopwatch = Stopwatch.StartNew();
            Console.WriteLine("START: Building Arcs...");
            MultiArcLoader sal = new MultiArcLoader(dirs1, dirs2, dirs3, null, 3, MultiArcLoader.LoadMethod.Single, false, null, true, MultiArcLoader.Exclude.None | MultiArcLoader.Exclude.BG | MultiArcLoader.Exclude.CSV |
                                                                                                                      MultiArcLoader.Exclude.Motion | MultiArcLoader.Exclude.Parts | MultiArcLoader.Exclude.PriorityMaterial |
                                                                                                                      MultiArcLoader.Exclude.Sound | MultiArcLoader.Exclude.System | MultiArcLoader.Exclude.Voice);
            sal.LoadArcs();
            stopwatch.Stop();
            Console.WriteLine("DONE: Building Arcs..." + stopwatch.Elapsed.ToString());

            Console.WriteLine("START: Collecting Labels...");
            stopwatch = Stopwatch.StartNew();

            List<string> names = new List<string>();

            Dictionary<string, List<string>> data = new Dictionary<string, List<string>>();
            foreach (ArcFileEntry arcFile in sal.arc.Files.Values)
            {
                if (arcFile.Name.EndsWith("." + "ks"))
                {
                    if (!(sal.GetContentsArcFilePath(arcFile) == null || sal.GetContentsArcFilePath(arcFile).Trim().Equals("")))
                    {
                        string path = arcFile.FullName;

                        if (path.Contains(@"\motion\") && (path.Contains(@"\sex\") || path.Contains(@"\m_sex\")))
                        {
                            string[] nameSplit = arcFile.Name.Split('.')[0].Split('_');
                            for (int i = 0; i < nameSplit.Length; i++)
                            {
                                if (!names.Contains(nameSplit[i]))
                                {
                                    names.Add(nameSplit[i]);
                                }
                            }
                        }
                    }
                }
            }

            stopwatch.Stop();
            Console.WriteLine("DONE: Collecting Labels..." + stopwatch.Elapsed.ToString());
            names.Sort();
            string csv = String.Join("\n", names.ToArray());
            

            Console.WriteLine("Press any key to exit");
        }

        #region
        static void ScriptCompleteAnalysis()
        {
            string[] dirs1 = new string[] { @"C:\KISS\CM3D2\GameData" };
            string[] dirs2 = new string[] { @"C:\KISS\COM3D2\GameData_20" };
            string[] dirs3 = new string[] { @"C:\KISS\COM3D2\GameData" };
            string[] dirs4 = new string[] { @"C:\KISS\COM3D2\Mod" };

            Stopwatch stopwatch;
            stopwatch = Stopwatch.StartNew();
            Console.WriteLine("START: Building Arcs...");
            MultiArcLoader mal = new MultiArcLoader(dirs1, dirs2, dirs3, null, 3, MultiArcLoader.LoadMethod.Single, false, null, true, MultiArcLoader.Exclude.None | MultiArcLoader.Exclude.BG | MultiArcLoader.Exclude.CSV |
                                                                                                                      MultiArcLoader.Exclude.Motion | MultiArcLoader.Exclude.Parts | MultiArcLoader.Exclude.PriorityMaterial |
                                                                                                                      MultiArcLoader.Exclude.Sound | MultiArcLoader.Exclude.System | MultiArcLoader.Exclude.Voice);
            mal.LoadArcs();
            stopwatch.Stop();
            Console.WriteLine("DONE: Building Arcs..." + stopwatch.Elapsed.ToString());

            List<string> unrecognizedCommands = new List<string>();
            Dictionary<string, List<string>> _MotionScripts = new Dictionary<string, List<string>>();
            Dictionary<string, List<string>> _TalkVoice = new Dictionary<string, List<string>>();
            Dictionary<string, List<string>> _TalkRepeatVoice = new Dictionary<string, List<string>>();

            Console.WriteLine("START: Collecting All Data...");
            stopwatch = Stopwatch.StartNew();
            //Loop files
            foreach (ArcFileEntry arcFile in mal.arc.Files.Values)
            {
                //Must be a script
                if (arcFile.Name.EndsWith("." + "ks"))
                {
                    if (!(mal.GetContentsArcFilePath(arcFile) == null || mal.GetContentsArcFilePath(arcFile).Trim().Equals("")))
                    {
                        string path = arcFile.FullName;
                        ArcDirectoryEntry parent = arcFile.Parent;
                        string arcPath = parent.ArcPath;
                        while(arcPath == null && parent.Parent != null)
                        {
                            parent = parent.Parent;
                            arcPath = parent.ArcPath;
                        }
                        if (!path.Contains("cbl"))
                        {
                            arcFile.Pointer = arcFile.Pointer.Decompress();
                            string script = NUty.SjisToUnicode(arcFile.Pointer.Data); //Encoding.UTF8.GetString(arcFile.Pointer.Data);
                            string[] lines = script.Split(new string[] { "\r\n" }, System.StringSplitOptions.None);

                            for (int i = 0; i < lines.Length; i++)
                            {
                                string line = lines[i].TrimStart().ToLower();

                                //Command
                                if (line.StartsWith(@"@", StringComparison.OrdinalIgnoreCase))
                                {
                                    //Special Cases
                                    string commandText = line.Contains(" ") ? line.Split(' ')[0].Trim() : line.Trim();
                                    switch (commandText)
                                    {
                                        case ScriptTag.ScriptCommand.talkrepeat:
                                        {
                                            ScriptTag tag = analyzeCommandStandard(ScriptTag.TalkRepeat, line, path, arcPath);
                                            if (tag.results.ContainsKey("voice") && tag.results["voice"].Keys.Count > 0)
                                            {
                                                string voiceFile = tag.results["voice"].Keys.ToList()[0];

                                                string nextLine = ((i + 1) < lines.Length) ? lines[i + 1] : "";
                                                if (!_TalkRepeatVoice.ContainsKey(voiceFile))
                                                {
                                                    _TalkRepeatVoice[voiceFile] = new List<string>();
                                                }
                                                _TalkRepeatVoice[voiceFile] = (new string[] { nextLine, path, line, arcPath }).ToList();
                                            }
                                            break;
                                        }
                                        case ScriptTag.ScriptCommand.talk:
                                        {
                                            ScriptTag tag = analyzeCommandStandard(ScriptTag.Talk, line, path, arcPath);
                                            if (tag.results.ContainsKey("voice") && tag.results["voice"].Keys.Count > 0)
                                            {
                                                string voiceFile = tag.results["voice"].Keys.ToList()[0];

                                                string nextLine = ((i + 1) < lines.Length) ? lines[i + 1] : "";
                                                if (!_TalkVoice.ContainsKey(voiceFile))
                                                {
                                                    _TalkVoice[voiceFile] = new List<string>();
                                                }
                                                _TalkVoice[voiceFile]=((new string[] { nextLine, path, line, arcPath }).ToList());
                                            }
                                            break;
                                        }
                                        case ScriptTag.ScriptCommand.motion:
                                        {
                                            analyzeCommandStandard(ScriptTag.Motion, line, path, arcPath);
                                            break;
                                        }
                                        default:
                                        {
                                            if (!unrecognizedCommands.Contains(commandText))
                                            {
                                                unrecognizedCommands.Add(commandText);
                                            }
                                            break;
                                        }
                                    }
                                }

                                //Label
                                else if (line.StartsWith(@"*", StringComparison.OrdinalIgnoreCase))
                                {
                                    //MotionScript
                                    if (path.Contains(@"\motion\"))
                                    {
                                        string key = arcFile.Name;

                                        if (!_MotionScripts.ContainsKey(key))
                                        {
                                            _MotionScripts[key] = new List<string>();
                                        }

                                        if (!_MotionScripts[key].Contains(line))
                                        {
                                            _MotionScripts[key].Add(line);
                                        }
                                    }
                                }
                            }

                        }
                    }
                    else
                    {
                        Console.WriteLine("LOST ARC: " + arcFile.FullName);
                    }
                }
            }
            stopwatch.Stop();
            Console.WriteLine("DONE: Collecting All Data..." + stopwatch.Elapsed.ToString());

            Console.WriteLine("getScriptHelpersSoundData: Start: Talk");
            stopwatch = Stopwatch.StartNew();
            List<List<string>> data = new List<List<string>>();
            foreach(KeyValuePair<string, List<string>> kvp in _TalkVoice)
            {
                string commandType = "Talk";
                string file = kvp.Key;
                string subtitles = _TalkVoice[file][0];
                string translated = subtitles;//Translate(subtitles);
                string examplePath = _TalkVoice[file][1];
                string example = _TalkVoice[file][2];

                //Row Contents
                List<string> row = new List<string>();
                row.Add(file);
                row.Add(commandType);
                row.Add(subtitles);
                row.Add(translated);
                row.Add(examplePath);
                row.Add(example);
                data.Add(row);

                if(kvp.Value[3] == null)
                {
                    Console.WriteLine(file);
                }
            }

            stopwatch.Stop();
            Console.WriteLine("getScriptHelpersSoundData: End: Talk");

            Console.WriteLine("Press any key to exit");
        }

        private static ScriptTag analyzeCommandStandard(ScriptTag tag, string line, string path, string arcPath)
        {
            ScriptTag tagReturn = new ScriptTag(tag.tag, tag.paramNames.ToArray());

            //Remove initial tag
            line = line.Substring(tag.tag.Length -1).Trim();
            string lineOriginal = line;

            //Replace all param names with alternative to fix missing spaces
            for (int j = 0; j < tag.paramNames.Count; j++)
            {
                line = line.Replace(tag.paramNames[j], "|" + tag.paramNames[j]);
            }

            List<string> paramsSplit = line.Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries).ToList();
            for (int j = 0; j < paramsSplit.Count; j++)
            {
                //Cleanup whitespace
                paramsSplit[j] = paramsSplit[j].Trim();

                //Collect
                if (paramsSplit[j].Contains("="))
                {
                    string[] paramDetails = paramsSplit[j].Split('=');

                    if (paramDetails.Length == 2)
                    {
                        string paramName = paramDetails[0].Trim().ToLower();
                        string paramValue = paramDetails[1].Trim().ToLower();

                        if (tag.paramNames.Contains(paramName + "="))
                        {
                            //If param needs dictionary of value->source
                            if (!tag.results.ContainsKey(paramName))
                            {
                                tag.results[paramName] = new Dictionary<string, string[]>();
                            }
                            if (!tagReturn.results.ContainsKey(paramName))
                            {
                                tagReturn.results[paramName] = new Dictionary<string, string[]>();
                            }

                            //If param does not have value yet
                            if (tag.results.ContainsKey(paramName) && !tag.results[paramName].ContainsKey(paramValue))
                            {
                                tag.results[paramName][paramValue] = new string[] { lineOriginal, path, arcPath };
                                tagReturn.results[paramName][paramValue] = new string[] { lineOriginal, path, arcPath };
                            }
                        }
                        else
                        {
                            tag.unknownParams.Add(new List<string>() { paramName, lineOriginal, path, arcPath });
                            tagReturn.unknownParams.Add(new List<string>() { paramName, lineOriginal, path, arcPath });
                        }
                    }
                    else
                    {
                        tag.badScripts.Add(new List<string>() { lineOriginal, path, arcPath });
                        tagReturn.badScripts.Add(new List<string>() { lineOriginal, path, arcPath });
                    }
                }
                else
                {
                    string paramName = paramsSplit[j];
                    string paramValue = paramName;

                    //If param needs dictionary of value->source
                    if (!tag.results.ContainsKey(paramName))
                    {
                        if (tag.paramNames.Contains(paramName))
                        {
                            tag.results[paramName] = new Dictionary<string, string[]>();
                            tagReturn.results[paramName] = new Dictionary<string, string[]>();
                        }
                        else
                        {
                            tag.unknownParams.Add(new List<string>() { paramName, lineOriginal, path, arcPath });
                            tagReturn.unknownParams.Add(new List<string>() { paramName, lineOriginal, path, arcPath });
                        }
                    }

                    //If param does not have value yet
                    if (tag.results.ContainsKey(paramName) && !tag.results[paramName].ContainsKey(paramValue))
                    {
                        tag.results[paramName][paramValue] = new string[] { lineOriginal, path, arcPath };
                        tagReturn.results[paramName][paramValue] = new string[] { lineOriginal, path, arcPath };
                    }
                }
            }

            return tagReturn;
        }
        #endregion

        public string ReplaceFirst(string text, string search, string replace)
        {
            int pos = text.IndexOf(search);
            if (pos < 0)
            {
                return text;
            }
            return text.Substring(0, pos) + replace + text.Substring(pos + search.Length);
        }

        static void IsExist()
        {
            Stopwatch stopwatch;

            //Load ARCs
            Console.WriteLine("START: Building Arcs build Cache...");
            stopwatch = Stopwatch.StartNew();

            string[] dirs1 = new string[] { @"C:\KISS\CM3D2\GameData" };
            string[] dirs2 = new string[] { @"C:\KISS\COM3D2\GameData_20" };
            string[] dirs3 = new string[] { @"C:\KISS\COM3D2\GameData" };
            string[] dirs4 = new string[] { @"C:\KISS\COM3D2\Mod" };

            MultiArcLoader mal = new MultiArcLoader(dirs1, dirs2, dirs3, dirs4, 3, MultiArcLoader.LoadMethod.Single, true, @"C:\Users\MC\source\repos\CM3D2.Toolkit\CM3D2.Toolkit\cachetest.json", false, MultiArcLoader.Exclude.None, new ConsoleLogger());
            mal.LoadArcs();
            stopwatch.Stop();
            Console.WriteLine("DONE: Building Arcs build Cache..." + stopwatch.Elapsed.ToString());

            Console.WriteLine("START: Building Arcs from Cache...");
            stopwatch = Stopwatch.StartNew();
            mal.LoadArcs();
            stopwatch.Stop();
            Console.WriteLine("DONE: Building Arcs from Cache..." + stopwatch.Elapsed.ToString());

            Console.WriteLine("START: Building Arcs don't build Cache...");
            stopwatch = Stopwatch.StartNew();
            mal = new MultiArcLoader(dirs1, dirs2, dirs3, dirs4, 3, MultiArcLoader.LoadMethod.Single, false, null, false, MultiArcLoader.Exclude.None);
            mal.LoadArcs();
            stopwatch.Stop();
            Console.WriteLine("DONE: Building Arcs don't build Cache..." + stopwatch.Elapsed.ToString());

            //string neifile = "casinoshop_data.nei";
            //List<List<string>> csv = NeiLib.NeiConverter.ToCSVList(mal.arc.Files[neifile].Pointer.Data);

            //Build HashSet
            Console.WriteLine("START: Building HashSet...");
            stopwatch = Stopwatch.StartNew();
            HashSet<string> fileNames = new HashSet<string>(mal.arc.Files.Keys);
            stopwatch.Stop();
            Console.WriteLine("DONE: Building HashSet..." + stopwatch.Elapsed.ToString());

            Console.WriteLine("HashSet: " + fileNames.Count);

            Console.WriteLine(fileNames.Contains("maid_status_yotogiclass_list.nei"));
            Console.ReadLine();
        }
    }

    public class ScriptTag
    {
        public string tag { get; set; }
        public List<string> paramNames { get; set; }
        public Dictionary<string, Dictionary<string, string[]>> results { get; set; }
        public List<List<string>> unknownParams { get; set; }
        public List<List<string>> badScripts { get; set; }
        public ScriptTag(string tag, string[] paramNames)
        {
            this.tag = tag;
            this.paramNames = paramNames.ToList<String>();
            results = new Dictionary<string, Dictionary<string, string[]>>();
            unknownParams = new List<List<string>>();
            badScripts = new List<List<string>>();
        }

        public static ScriptTag Talk = new ScriptTag(@"@talk ", new string[] { "name=", "real=", "voice=", "maid=", "man=", "np" });
        public static ScriptTag TalkRepeat = new ScriptTag(@"@talkrepeat ", new string[] { "name=", "real=", "voice=", "maid=", "man=", "np" });
        public static ScriptTag Face = new ScriptTag(@"@face ", new string[] { "maid=", "name=", "wait=" });
        public static ScriptTag FaceBlend = new ScriptTag(@"@faceblend ", new string[] { "maid=", "man=", "name=" });
        public static ScriptTag FaceBlend2 = new ScriptTag(@"@faceblend2 ", new string[] { "maid=", "man=", "name=" });
        public static ScriptTag Motion = new ScriptTag(@"@motion ", new string[] { "maid=", "man=", "mot=", "blend=", "loop=", "wait=" });
        public static ScriptTag MotionScript = new ScriptTag(@"@motionscript ", new string[] { "maid=", "man=", "file=", "label=", "wait=", "sloat=", "npos", "bodymix" });
        public static ScriptTag PhysicsHit = new ScriptTag(@"@phisicshit ", new string[] { "maid=", "man=", "height="});
        public static ScriptTag TexMulAdd = new ScriptTag(@"@texmuladd ", new string[] { "layer=", "x=", "y=", "z=", "r=", "s=", "slot=", "matno=", "propname=", "file=", "res=", "part=", "delay="});
        public static ScriptTag EyeToPosition = new ScriptTag(@"@eyetoposition ", new string[] { "maid=", "man=", "x=", "y=", "z=", "blend=" });
        public static ScriptTag PlayVoice = new ScriptTag(@"@playvoice ", new string[] { "maid=", "voice=", "name=", "wait=", "wait" });
        public static ScriptTag PlaySE = new ScriptTag(@"@playse ", new string[] { "file=", "loop", "wait=", "wait"});

        public string buildCSV()
        {
            //Get Row Count
            int rowCount = 0;
            for (int i = 0; i < this.results.Keys.ToList().Count; i++)
            {
                rowCount = Math.Max(rowCount, this.results[this.results.Keys.ToList()[i]].Values.Count) + 1;
            }

            //Build Header
            string[] extraColumns = { "SCRIPT", "FILE" };
            string[] header = new string[this.results.Keys.ToList().Count * (extraColumns.Length + 1)];
            for (int i = 0; i < this.results.Keys.ToList().Count; i++)
            {
                string key = this.results.Keys.ToList()[i];
                int startingCol = i * (extraColumns.Length + 1);

                header[startingCol] = key;
                for(int j=0; j< extraColumns.Length; j++)
                {
                    header[startingCol + j + 1] = extraColumns[j];
                }
            }

            //Build Contents
            string[] contents = new string[rowCount];
            for (int j = 0; j < rowCount; j++)
            {
                //Build row
                string[] row = new string[this.results.Keys.ToList().Count * (extraColumns.Length + 1)];
                for (int i = 0; i < this.results.Keys.ToList().Count; i++)
                {
                    string key = this.results.Keys.ToList()[i];
                    int startingCol = i * (extraColumns.Length + 1);

                    //If this tag has data for this row
                    if (j < this.results[key].Keys.Count)
                    {
                        row[startingCol] = this.results[key].Keys.ToList()[j];
                        for (int k = 0; k < extraColumns.Length; k++)
                        {
                            row[startingCol + k + 1] = this.results[key][this.results[key].Keys.ToList()[j]][k];
                        }
                    }
                    else
                    {
                        row[startingCol] = "NULL";
                        for (int k = 0; k < extraColumns.Length; k++)
                        {
                            row[startingCol + k + 1] = "NULL";
                        }
                    }
                }

                //Place in Contents
                contents[j] = string.Join("|", row);
            }

            return string.Join("|", header) + "\n" + string.Join("\n", contents);

            //string csv = "";
            //for (int i = 0; i < this.results.Keys.ToList().Count; i++)
            //{
            //    //Append column delimitter
            //    if (!csv.Equals(""))
            //    {
            //        csv += "|";
            //    }

            //    //Append data
            //    csv += this.results.Keys.ToList()[i] + "|SCRIPT|FILE";
            //}
            //csv += "\n";

            ////Tasks
            //int threadCount = 5;
            //Task[] tasks = new Task[threadCount];
            //for (int i = 0; i < threadCount; i++)
            //{
            //    tasks[i] = Task.Factory.StartNew(buildCSV_Task, i.ToString());
            //}
            //Task.WaitAll(tasks);


            ////After finishing threads, merge strings
            //arc = arcFilePathsDividedArcs[0];
            //for (int i = 1; i < threadCount; i++)
            //{
            //    arc.MergeCopy(arcFilePathsDividedArcs[i].Root, arc.Root);
            //}

            //for (int j = 0; j < rowCount; j++)
            //{
            //    for (int i = 0; i < tag.results.Keys.ToList().Count; i++)
            //    {
            //        string key = tag.results.Keys.ToList()[i];

            //        if (i != 0)
            //        {
            //            csv += "|";
            //        }

            //        if (j < tag.results[key].Keys.Count)
            //        {
            //            csv += tag.results[key].Keys.ToList()[j] + "|" + tag.results[key][tag.results[key].Keys.ToList()[j]][0] + "|" + tag.results[key][tag.results[key].Keys.ToList()[j]][1];
            //        }
            //        else
            //        {
            //            csv += "NULL|NULL|NULL";
            //        }
            //    }
            //    csv += "\n";
            }

        public class ScriptCommand
        {
            public const string talk = @"@talk";
            public const string talkrepeat = @"@talkrepeat";
            public const string face = @"@face";
            public const string faceblend = @"@faceblend";
            public const string faceblend2 = @"@faceblend2";
            public const string motion = @"@motion";
            public const string phisicshit = @"@phisicshit";
            public const string texmuladd = @"@texmuladd";
            public const string eyetoposition = @"@eyetoposition";
            public const string playse = @"@playse";
        }

    }

    //private void buildCSV_Task(System.Object i)
    //{
    //    int index = Int32.Parse(i as string);





    //    List<string> arcFilePaths = arcFilePathsDivided[index];

    //    ArcFileSystem afs = new ArcFileSystem("root", heirarchyOnly, keepDupes);

    //    if (arcFilePaths.Count > 0)
    //    {
    //        //Loop paths
    //        foreach (String arcFilePath in arcFilePaths)
    //        {
    //            //Load into the next arc
    //            //Console.WriteLine("Task " + index + ": " + arcFilePath);

    //            switch (loadMethod)
    //            {
    //                case LoadMethod.Single:
    //                    {
    //                        string arcName = Path.GetFileNameWithoutExtension(arcFilePath);
    //                        ArcDirectoryEntry dir = afs.CreateDirectory(arcName, afs.Root);
    //                        afs.LoadArc(arcFilePath, dir, true);
    //                        break;
    //                    }
    //                case LoadMethod.MiniTemps:
    //                    {
    //                        ArcFileSystem afsTemp = new ArcFileSystem(heirarchyOnly, keepDupes);
    //                        afsTemp.LoadArc(arcFilePath, afsTemp.Root, true);

    //                        //Combine to shared arc
    //                        afs.MergeCopy(afsTemp.Root, afs.Root);
    //                        break;
    //                    }
    //                    //case LoadMethod.SingleIgnoreArcNames:
    //                    //    {
    //                    //        afs.LoadArc(arcFilePath, afs.Root);
    //                    //        break;
    //                    //    }
    //            }
    //        }
    //    }

    //    //Copy out
    //    arcFilePathsDividedArcs[index] = afs;
    //}
    //}

    public class ConsoleLogger : ILogger
    {
        public string Name => "MultiArcConsoleLogger";
        private static string GetTimeStamp() => DateTime.Now.ToString("yy/mm/dd-hh:MM:ss");

        /// <inheritdoc />
        public void Debug(string message, params object[] args)
        {
            if (!false)
                return;
            var msg = string.Format(message, args);
            using (new ConsoleColorSwitch(ConsoleColor.Cyan, ConsoleColor.Black))
                Console.WriteLine($"[{Name}] [DEBUG] {GetTimeStamp()} - {msg}");
        }

        /// <inheritdoc />
        public void Error(string message, params object[] args)
        {
            if (!true)
                return;
            var msg = string.Format(message, args);
            using (new ConsoleColorSwitch(ConsoleColor.DarkRed, ConsoleColor.Black))
                Console.WriteLine($"[{Name}] [ERROR] {GetTimeStamp()} - {msg}");
        }

        /// <inheritdoc />
        public void Info(string message, params object[] args)
        {
            //var msg = string.Format(message, args);
            //using (new ConsoleColorSwitch(ConsoleColor.White, ConsoleColor.Black))
            //    Console.WriteLine($"[{Name}] [INFO ] {GetTimeStamp()} - {msg}");
        }

        /// <inheritdoc />
        public void Trace(string message, params object[] args)
        {
            if (!false)
                return;
            var msg = string.Format(message, args);
            using (new ConsoleColorSwitch(ConsoleColor.DarkGray, ConsoleColor.Black))
                Console.WriteLine($"[{Name}] [TRACE] {GetTimeStamp()} - {msg}");
        }

        /// <inheritdoc />
        public void Warn(string message, params object[] args)
        {
            if (!true)
                return;
            var msg = string.Format(message, args);
            using (new ConsoleColorSwitch(ConsoleColor.Yellow, ConsoleColor.Black))
                Console.WriteLine($"[{Name}] [WARN ] {GetTimeStamp()} - {msg}");
        }

        /// <inheritdoc />
        public void Fatal(string message, params object[] args)
        {
            if (!true)
                return;
            var msg = string.Format(message, args);
            using (new ConsoleColorSwitch(ConsoleColor.White, ConsoleColor.Red))
                Console.WriteLine($"[{Name}] [FATAL] {GetTimeStamp()} - {msg}");
        }

        private class ConsoleColorSwitch : IDisposable
        {
            private ConsoleColor bgCol;
            private ConsoleColor fgCol;

            public ConsoleColorSwitch(ConsoleColor fg, ConsoleColor bg)
            {
                fgCol = Console.ForegroundColor;
                bgCol = Console.BackgroundColor;
                Console.ForegroundColor = fg;
                Console.BackgroundColor = bg;
            }

            public void Dispose()
            {
                Console.ForegroundColor = fgCol;
                Console.BackgroundColor = bgCol;
            }
        }
    }
}
