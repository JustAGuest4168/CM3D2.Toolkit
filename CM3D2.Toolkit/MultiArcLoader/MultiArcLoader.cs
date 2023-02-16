using CM3D2.Toolkit.Guest4168Branch.Arc;
using CM3D2.Toolkit.Guest4168Branch.Arc.Entry;
using CM3D2.Toolkit.Guest4168Branch.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CM3D2.Toolkit.Guest4168Branch.MultiArcLoader
{
    public class MultiArcLoader : IDisposable
    {
        //Instance Properties
        string[] directoriesCM { get; set; }
        string[] directoriesCOM20 { get; set; }
        string[] directoriesCOM { get; set; }
        string[] directoriesCOMMods { get; set; }
        int threadCount;
        Exclude exclusions;
        bool hierarchyOnly;
        string hierarchyPath;
        bool keepDupes;
        LoadMethod loadMethod;
        ILogger log;

        //Thread Properties
        List<List<string>> arcFilePathsDivided;
        List<ArcFileSystem> arcFilePathsDividedArcs;
        public ArcFileSystem arc;
        public List<string> arcFilePaths { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="cmGameData"></param>
        /// <param name="comGameData20"></param>
        /// <param name="comGameData"></param>
        /// <param name="comMods"></param>
        /// <param name="threads"></param>
        /// <param name="loadingMethod"></param>
        /// <param name="hierarchyOnlyFromCache"></param>
        /// <param name="hierachyCachePath"></param>
        /// <param name="keepDuplicates"></param>
        /// <param name="exclude"></param>
        /// <param name="logger"></param>
        public MultiArcLoader(string[] cmGameData, string[] comGameData20, string[] comGameData, string[] comMods, int threads, LoadMethod loadingMethod = LoadMethod.Single, bool hierarchyOnlyFromCache = false, string hierachyCachePath = null, bool keepDuplicates = false, Exclude exclude = Exclude.None, ILogger logger = null)
        {
            directoriesCM = cmGameData;
            directoriesCOM20 = comGameData20;
            directoriesCOM = comGameData;
            directoriesCOMMods = comMods;
            threadCount = Math.Max(1, threads);
            loadMethod = loadingMethod;
            hierarchyOnly = hierarchyOnlyFromCache;
            hierarchyPath = hierachyCachePath;
            keepDupes = keepDuplicates;
            exclusions = exclude;
            log = logger;

            if (log == null)
            {
                log = NullLogger.Instance;
            }
        }

        #region Additional Constructors
        public MultiArcLoader(string[] cmGameData, string[] comGameData20, string[] comGameData, string[] comMods, int threads, LoadMethod loadingMethod = LoadMethod.Single, bool keepDuplicates = false, Exclude exclude = Exclude.None, ILogger logger = null) : this(cmGameData, comGameData20, comGameData, comMods, threads, loadingMethod, false, null, keepDuplicates, exclude, logger)
        {
        }

        public MultiArcLoader(string[] cmGameData, string[] comGameData20, string[] comGameData, string[] comMods, int threads, LoadMethod loadingMethod = LoadMethod.Single, bool hierarchyOnlyFromCache = false, bool keepDuplicates = false, Exclude exclude = Exclude.None) : this(cmGameData, comGameData20, comGameData, comMods, threads, loadingMethod, hierarchyOnlyFromCache, null, keepDuplicates, exclude, null)
        {
        }
        public MultiArcLoader(string[] cmGameData, string[] comGameData20, string[] comGameData, string[] comMods, int threads, LoadMethod loadingMethod = LoadMethod.Single, bool hierarchyOnlyFromCache = false, bool keepDuplicates = false, Exclude exclude = Exclude.None, ILogger logger = null) : this(cmGameData, comGameData20, comGameData, comMods, threads, loadingMethod, hierarchyOnlyFromCache, null, keepDuplicates, exclude, logger)
        {
        }
        #endregion


        public void LoadArcs()
        {
            //Combine all file paths
            arcFilePaths = new List<string>();
            if (directoriesCM != null)
            {
                arcFilePaths.AddRange(fetchSortedArcs(directoriesCM, exclusions, log));
            }
            if (directoriesCOM20 != null)
            {
                arcFilePaths.AddRange(fetchSortedArcs(directoriesCOM20, exclusions, log));
            }
            if (directoriesCOM != null)
            {
                arcFilePaths.AddRange(fetchSortedArcs(directoriesCOM, exclusions, log));
            }
            if (directoriesCOMMods != null)
            {
                arcFilePaths.AddRange(fetchSortedArcs(directoriesCOMMods, exclusions, log));
            }

            //Build from cache if possible
            if (hierarchyOnly && hierarchyPath != null)
            {
                //File Exists
                if (System.IO.File.Exists(hierarchyPath))
                {
                    //Load from file
                    MultiArcLoaderHierarchyCache cache = Newtonsoft.Json.JsonConvert.DeserializeObject<MultiArcLoaderHierarchyCache>(File.ReadAllText(hierarchyPath));
                    List<string> cacheFiles = cache.data.Keys.ToList<string>();
                    cacheFiles.Sort();

                    //List of arcs match
                    if (cacheFiles.SequenceEqual(arcFilePaths))
                    { 
                        bool buildFromCache = true;

                        //Loop the arc files from cache
                        foreach (KeyValuePair<string, MultiArcLoaderHierarchyCache.Data> cacheKvp in cache.data)
                        {
                            string arcPath = cacheKvp.Key;
                            
                            //File still exists
                            if (System.IO.File.Exists(arcPath))
                            {
                                //Check date modified
                                FileInfo fi = new FileInfo(arcPath);
                                if (!fi.LastWriteTimeUtc.Equals(cacheKvp.Value.dte))
                                {
                                    log.Info(String.Format("MultiArcLoader: ARC File has been modified (UTC) since last cache:{0} Cache: {1} File: {2}", arcPath, cacheKvp.Value.dte.ToString(), fi.LastWriteTimeUtc));
                                    buildFromCache = false;
                                    break;
                                }
                            }
                            else
                            {
                                log.Error(String.Format("MultiArcLoader: ARC File not found:{0}", arcPath));
                                buildFromCache = false;
                                break;
                            }
                        }

                        //Build the actual ARC
                        if (buildFromCache)
                        {
                            arc = buildHierarchyOnlyArc(cache);
                            return;
                        }
                    }
                    else
                    {
                        log.Error(String.Format("MultiArcLoader: ARC File list does not match"));
                    }
                }
                else
                {
                    log.Info(String.Format("MultiArcLoader: ARC Hierarchy File not found:{0}", hierarchyPath));
                }

                log.Info("MultiArcLoader: LoadArcs will now build the full data");
            }

            //Divide work up based on size
            //Could have used some object, but i'm lazy so multiple lists
            arcFilePathsDivided = new List<List<string>>();
            arcFilePathsDividedArcs = new List<ArcFileSystem>();
            List<long> arcFilePathsDividedSizes = new List<long>();
            for (int i = 0; i < threadCount; i++)
            {
                arcFilePathsDivided.Add(new List<string>());
                arcFilePathsDividedArcs.Add(null);
                arcFilePathsDividedSizes.Add(0);
            }

            //Loop all arcs
            foreach (string filePath in arcFilePaths)
            {
                FileInfo nextFile = new FileInfo(filePath);

                //Smallest collection gets next arc
                int threadIndex = arcFilePathsDividedSizes.IndexOf(arcFilePathsDividedSizes.Min());
                arcFilePathsDivided[threadIndex].Add(filePath);
                arcFilePathsDividedSizes[threadIndex] = arcFilePathsDividedSizes[threadIndex] + nextFile.Length;
            }

            //Tasks
            Task[] tasks = new Task[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                tasks[i] = Task.Factory.StartNew(loadArcsInTask, i.ToString());
            }

            Task.WaitAll(tasks);


            //After finishing threads, copy everything to single ARC
            arc = null; 
            for (int i = 0; i < threadCount; i++)
            {
                if (arc == null)
                {
                    arc = arcFilePathsDividedArcs[i];
                }
                else if(arcFilePathsDividedArcs[i] != null)
                {
                    arc.MergeCopy(arcFilePathsDividedArcs[i].Root, arc.Root);
                }
            }

            //Build a cache if necessary
            if (hierarchyPath != null)
            {
                MultiArcLoaderHierarchyCache hierarchy = new MultiArcLoaderHierarchyCache();

                //Loop file paths
                foreach (string filePath in arcFilePaths)
                {
                    //Add arc file name if necessary
                    if (!hierarchy.data.ContainsKey(filePath))
                    {
                        hierarchy.data[filePath] = new MultiArcLoaderHierarchyCache.Data();
                        hierarchy.data[filePath].dte = new FileInfo(filePath).LastWriteTimeUtc;
                    }
                }

                //Loop files
                foreach (KeyValuePair<string, ArcFileEntry> kvp in arc.Files)
                {
                    string filePath = ((Arc.FilePointer.ArcFilePointer)kvp.Value.Pointer).ArcFile;

                    //Add arc file name if necessary
                    if (!hierarchy.data.ContainsKey(filePath))
                    {
                        hierarchy.data[filePath] = new MultiArcLoaderHierarchyCache.Data();
                        hierarchy.data[filePath].dte = new FileInfo(filePath).LastWriteTimeUtc;
                    }

                    //Add files contained in arc
                    hierarchy.data[filePath].files.Add(kvp.Value.FullName);
                }

                //Write file
                File.WriteAllText(hierarchyPath, Newtonsoft.Json.JsonConvert.SerializeObject(hierarchy));
            }
        }

        private static List<string> fetchSortedArcs(string[] dirs, Exclude exclusions, ILogger log)
        {
            List<string> list = new List<string>();

            foreach (string dir in dirs)
            {
                //Passed arc directly
                if (dir.EndsWith(".arc"))
                {
                    string fileName = Path.GetFileName(dir);
                    if (includeArc(fileName, exclusions, log))
                    {
                        list.Add(dir);
                    }
                }
                //Passed directory
                else
                {
                    string[] files = Directory.GetFiles(dir, "*.arc", SearchOption.AllDirectories);
                    foreach (string file in files)
                    {
                        string fileName = Path.GetFileName(file);
                        if (includeArc(fileName, exclusions, log))
                        {
                            list.Add(file);
                        }
                    }
                }
            }
            list.Sort();

            return list;
        }
        private static bool includeArc(string arcFileName, Exclude exclusions, ILogger log)
        {
            arcFileName = arcFileName.ToLower().Trim();

            if ((exclusions & Exclude.None) == Exclude.None)
            {
                return true;
            }

            if ((exclusions & Exclude.BG) == Exclude.BG && arcFileName.StartsWith("bg"))
            {
                return false;
            }
            if ((exclusions & Exclude.CSV) == Exclude.CSV && arcFileName.StartsWith("csv"))
            {
                return false;
            }
            if ((exclusions & Exclude.Motion) == Exclude.Motion && arcFileName.StartsWith("motion"))
            {
                return false;
            }
            if ((exclusions & Exclude.Parts) == Exclude.Parts && arcFileName.StartsWith("parts"))
            {
                return false;
            }
            if ((exclusions & Exclude.PriorityMaterial) == Exclude.PriorityMaterial && arcFileName.StartsWith("prioritymaterial"))
            {
                return false;
            }
            if ((exclusions & Exclude.Script) == Exclude.Script && arcFileName.StartsWith("script"))
            {
                return false;
            }
            if ((exclusions & Exclude.Sound) == Exclude.Sound && arcFileName.StartsWith("sound"))
            {
                return false;
            }
            if ((exclusions & Exclude.System) == Exclude.System && arcFileName.StartsWith("system"))
            {
                return false;
            }
            if ((exclusions & Exclude.Voice) == Exclude.Voice && arcFileName.StartsWith("voice"))
            {
                return false;
            }

            return true;
        }
        private void loadArcsInTask(System.Object i)
        {
            int index = Int32.Parse(i as string);
            List<string> arcFilePaths = arcFilePathsDivided[index];

            try
            {
                ArcFileSystem afs = new ArcFileSystem("root", keepDupes);
                afs.Logger = log;

                if (arcFilePaths.Count > 0)
                {
                    //Loop paths
                    foreach (String arcFilePath in arcFilePaths)
                    {
                        //Load into the next arc
                        //Console.WriteLine("Task " + index + ": " + arcFilePath);

                        switch (loadMethod)
                        {
                            case LoadMethod.Single:
                            {
                                string arcName = Path.GetFileNameWithoutExtension(arcFilePath);
                                ArcDirectoryEntry dir = afs.CreateDirectory(arcName, afs.Root);
                                try
                                {
                                    afs.LoadArc(arcFilePath, dir, true);
                                }
                                catch(Exception ex)
                                {
                                    log.Error("Unhandled Exception:{0}", ex.ToString());
                                }
                                break;
                            }
                            case LoadMethod.MiniTemps:
                            {
                                ArcFileSystem afsTemp = new ArcFileSystem(hierarchyPath, keepDupes);
                                afsTemp.Logger = log;
                                try
                                {
                                    afsTemp.LoadArc(arcFilePath, afsTemp.Root, true);

                                    //Combine to shared arc
                                    afs.MergeCopy(afsTemp.Root, afs.Root);
                                }
                                catch (Exception ex)
                                {
                                    log.Error("Unhandled Exception:{0}", ex.ToString());
                                }
                                
                                break;
                            }
                            //case LoadMethod.SingleIgnoreArcNames:
                            //    {
                            //        afs.LoadArc(arcFilePath, afs.Root);
                            //        break;
                            //    }
                        }
                    }
                }

                //Copy out
                arcFilePathsDividedArcs[index] = afs;
            }
            catch(Exception ex)
            {
                log.Error("Unhandled Exception:{0}", ex.ToString());
                arcFilePathsDividedArcs[index] = null;
            }
        }

        //Post Load Methods
        public string[] GetFileListAtExtension(string extension)
        {
            extension = extension.Trim();
            extension = (extension.StartsWith(".") ? extension.Substring(1) : extension).Trim();

            List<string> data = new List<string>();
            if (arc != null)
            {
                foreach (ArcFileEntry arcFile in arc.Files.Values)
                {
                    if (arcFile.Name.EndsWith("." + extension))
                    {
                        data.Add(arcFile.Name);
                    }
                }
            }

            return data.ToArray();
        }

        public string GetContentsArcFilePath(ArcEntryBase content)
        {
            if (content == null)
            {
                return null;
            }

            if (content.IsFile())
            {
                if (content.Parent != null)
                {
                    return this.GetContentsArcFilePath(content.Parent);
                }

                return null;
            }
            else
            {
                if (((ArcDirectoryEntry)content).Depth == 1)
                {
                    return ((ArcDirectoryEntry)content).ArcPath;
                }

                if (((ArcDirectoryEntry)content).Parent != null)
                {
                    return this.GetContentsArcFilePath(content.Parent);
                }

                return null;
            }
        }

        [System.Flags]
        public enum Exclude
        {
            None = (2 ^ 1),
            BG = (2 ^ 2),
            CSV = (2 ^ 3),
            Motion = (2 ^ 4),
            Parts = (2 ^ 5),
            PriorityMaterial = (2 ^ 6),
            Script = (2 ^ 7),
            Sound = (2 ^ 8),
            System = (2 ^ 0),
            Voice = (2 ^ 10)
        }

        public enum LoadMethod
        {
            Single = 0,
            MiniTemps = 1,
            //SingleIgnoreArcNames = 2
        }

        public class MultiArcLoaderHierarchyCache
        {
            public Dictionary<string, Data> data { get; set; }
            public MultiArcLoaderHierarchyCache()
            {
                data = new Dictionary<string, Data>();
            }

            public class Data
            {
                public DateTime dte { get; set; }
                public List<string> files { get; set; }
                public Data()
                {
                    dte = DateTime.MinValue;
                    files = new List<string>();
                }
            }
        }

        private static ArcFileSystem buildHierarchyOnlyArc(MultiArcLoaderHierarchyCache cache)
        {
            int pathPrefix = (@"CM3D2ToolKit:"+ Path.DirectorySeparatorChar + Path.DirectorySeparatorChar).Length;
            string root = null;
            foreach(KeyValuePair<string, MultiArcLoaderHierarchyCache.Data> kvpArc in cache.data)
            {
                if (kvpArc.Value.files.Count > 0)
                {
                    root = kvpArc.Value.files[0].Substring(pathPrefix).Split(Path.DirectorySeparatorChar)[0];
                    break;
                }
            }
            ArcFileSystem arcH = new ArcFileSystem(root, false);
            foreach(KeyValuePair<string, MultiArcLoaderHierarchyCache.Data> kvpArc in cache.data)
            {
                foreach(string fullPath in kvpArc.Value.files)
                {
                    string fixedPath = fullPath.Substring(pathPrefix).Substring(root.Length);
                    ArcFileEntry newFile = arcH.CreateFile(fixedPath);
                }
            }

            return arcH;
        }

        public void Dispose()
        {
            Dispose(true);

            GC.SuppressFinalize(this);
        }

        private bool disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    if (arc != null)
                    {
                        _dispose(arc.Root);
                    }
                }
                this.disposed = true;
            }
        }
        protected static void _dispose(ArcDirectoryEntry dir)
        {
            //Dispose Files
            foreach (ArcFileEntry file in dir.Files.Values)
            {
                file.Pointer.Dispose();
            }
            dir.Files.Clear();

            //Dispose Files in Directories
            foreach (ArcDirectoryEntry dir2 in dir.Directories.Values)
            {
                _dispose(dir2);
            }
            dir.Directories.Clear();
        }
    }
}