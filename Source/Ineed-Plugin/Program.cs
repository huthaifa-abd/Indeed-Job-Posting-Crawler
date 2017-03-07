using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Indeed_Plugin
{
    class Program
    {
        static void Main(string[] args)
        {
            //Setup Default Values

            //The local folder name to be used for storing 
            string mapStoragePath = "MapStorage";
            string candidatesStoragePath = "CandidatesStorage";
            //Number of pages to crawl
            int pageCount = 50;
            //Number of items per page to collect
            int itemsPerPage = 10;
            //Keywords
            string searchKeywords = "software+developer";

            //Command Line Args for Remote / Distributed Use 
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "-mapStoragePath":
                        mapStoragePath = args[i + 1];
                        break;

                    case "-candidatesStoragePath":
                        candidatesStoragePath = args[i + 1];
                        break;

                    case "-pageCount":
                        pageCount = StrToIntDef(args[i + 1], pageCount);
                        break;

                    case "-itemsPerPage":
                        itemsPerPage = StrToIntDef(args[i + 1], itemsPerPage);
                        break;

                    case "-searchKeywords":
                        searchKeywords = args[i + 1];
                        break;
                }
            }

            //A direct refrence to threads for status checks
            List<Thread> processingThreads = new List<Thread>();

            //Check if the map storage directory exists

            bool exists = System.IO.Directory.Exists(mapStoragePath);
            //Create a new folder if it doesn't 
            if (!exists)
                System.IO.Directory.CreateDirectory(mapStoragePath);
            else
            {
                //Delete all files
                DirectoryInfo dir = new DirectoryInfo(mapStoragePath);
                foreach (FileInfo file in dir.GetFiles())
                {
                    file.Delete();
                }
            }

            Console.WriteLine("Node Initialized Successfully ..");

            //Create Map Job Swarm (Release the beasts)
            //Each Thread will scan 1 page
            for (int i = 1; i <= pageCount; i++)
            {
                Thread mapThread = new Thread(() => indeedCrawler.MapJobs(i, i, itemsPerPage, searchKeywords, mapStoragePath));
                mapThread.Start();
                //Add thread to thread list to be monitored later
                processingThreads.Add(mapThread);
            }

            //Check if all mapping swarm has completed collecting information
            while (!processingThreads.All(t => t.IsAlive == false))
            {
                Console.WriteLine("Mapping Jobs Running : " + processingThreads.Count(t => t.IsAlive == true));
                //Wait for some time
                Thread.Sleep(1000);
            }

            Console.WriteLine("All Mapping Jobs Has Completed Execution Successfully");

            Console.WriteLine("Running Reduce Job to Combine Maps");

            //Run the reducer job to parse and combine mapping jobs results
            Thread reduceThread = new Thread(() => indeedCrawler.ReduceJobs(mapStoragePath, candidatesStoragePath));
            reduceThread.Start();
            while (reduceThread.IsAlive)
            {
                //Wait for some time
                Thread.Sleep(1000);
            }

        }
        /// <summary>
        /// Helper function that checks string input against integer value and return the defult value in case of failure 
        /// </summary>
        /// <param name="s"></param>
        /// <param name="default"></param>
        /// <returns></returns>
        public static int StrToIntDef(string s, int @default)
        {
            int number;
            if (int.TryParse(s, out number))
                return number;
            return @default;
        }
    }
}
