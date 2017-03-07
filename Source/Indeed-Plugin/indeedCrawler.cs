using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Indeed_Plugin
{
    public class indeedCrawler
    {
        //Specify The Base URL to collect job information
        static string baseURL = "https://www.indeed.com";

        /// <summary>
        ///Mapping Fucntion that crawls the web and collects job information
        /// </summary>
        /// <param name="threadIndex"></param>
        /// <param name="pageIndex"></param>
        /// <param name="itemCount"></param>
        /// <param name="searchKeywords"></param>
        /// <param name="mapStoragePath"></param>
        public static void MapJobs(int threadIndex, int pageIndex, int itemCount, string searchKeywords, string mapStoragePath)
        {
            try
            {
                //Create new web document object
                HtmlWeb web = new HtmlWeb();
                //Build the crawling URL
                HtmlDocument doc = web.Load(baseURL + "/jobs?q=" + searchKeywords + "&start=" + pageIndex);
                using (WebClient client = new WebClient())
                {
                    //Loop Page Items
                    for (int j = 0; j <= itemCount; j++)
                    {
                        //Object to store mapped values
                        string mapInfo = "";
                        //Default Saperator between values
                        string separator = " ";
                        //Xpath to locate points of interest in the document
                        var nodes = doc.DocumentNode.SelectSingleNode("//td[@id='resultsCol']/div[" + j + "]");

                        if (nodes != null)
                        {
                            string JobTitle = "";
                            Guid JobID = Guid.NewGuid();
                            //Search and get the Job title from the job object
                            if (nodes.SelectSingleNode("h2/a") != null)
                                mapInfo += JobID + "-" + cleanString(nodes.SelectSingleNode("h2/a").InnerText) + separator + ":" + " HUTHAIFA ";
                            else
                                mapInfo += "none";

                            //Set Job Title 
                            JobTitle = mapInfo;

                            //Search and get the Company Name from the job object
                            if (nodes.SelectSingleNode("span[contains(@class, 'company')]/span/a") != null)
                                mapInfo += cleanString(nodes.SelectSingleNode("span[contains(@class, 'company')]/span/a").InnerText) + separator;
                            else
                                mapInfo += "none" + separator;

                            //Search and get the Location from the job object
                            if (nodes.SelectSingleNode("span/span[contains(@class, 'location')]/span") != null)
                                mapInfo += cleanString(nodes.SelectSingleNode("span/span[contains(@class, 'location')]/span").InnerText) + separator;
                            else
                                mapInfo += "none" + separator;

                            //Search and get the Job Summary from the job object
                            if (nodes.SelectSingleNode("table/tr/td/div/span") != null)
                                mapInfo += cleanString(nodes.SelectSingleNode("table/tr/td/div/span").InnerText) + separator;
                            else
                                mapInfo += "none" + separator;

                            //Search and get the Rating from the job object
                            if (nodes.SelectSingleNode("a/span[contains(@class, 'rating')]/span") != null)
                                mapInfo += getRating(nodes.SelectSingleNode("a/span[contains(@class, 'rating')]/span").GetAttributeValue("style", "0")) + separator;
                            else
                                mapInfo += "none" + separator;

                            //Add Generation Time to put a hustorical metric to the data
                            mapInfo += "tick-" + DateTime.Now.Ticks + separator;

                            //Remove Spechil Characters
                            mapInfo = RemoveSpecialCharacters(mapInfo);
                            //Save Mapped information to file NOTE : USE RAM DISK PERFORMANCE IMPROVEMENT
                            if (JobTitle != "none")
                                System.IO.File.WriteAllText(mapStoragePath + @"/MapFile-p" + pageIndex + "-m" + j + ".txt", mapInfo);
                        }


                    }
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Thread Abort Exception");
            }
            finally
            {
            }
        }

        static string filterBySkills(string text)
        {
            string filtered = "";
            Dictionary<string, List<string>> jobs = new Dictionary<string, List<string>>();
            Dictionary<string, List<string>> keywords = new Dictionary<string, List<string>>();

            string[] lines = text.Split(new string[] { Environment.NewLine }, StringSplitOptions.None);


            using (StringReader reader = new StringReader(text))
            {
                string line = string.Empty;
                do
                {
                    line = reader.ReadLine();
                    if (!string.IsNullOrEmpty(line))
                    {
                        line = line.ToUpper();
                        string[] words = line.Split(':');
                        line = line.Substring(line.IndexOf(':') + 1);
                        string jobTitle = cleanString(words[0]).Trim().Replace(' ', '-');
                        if (!jobs.ContainsKey(jobTitle))
                            jobs.Add(jobTitle, line.Replace(jobTitle, "").Trim().Split(' ').Distinct().ToList());
                    }

                } while (line != null);
            }

            using (StringReader reader = new StringReader(text))
            {
                string line = string.Empty;
                do
                {
                    line = reader.ReadLine();
                    if (!string.IsNullOrEmpty(line))
                    {
                        line = line.ToUpper();
                        string[] words = line.Split(':');
                        line = line.Substring(line.IndexOf(':') + 1);
                        string keywordItem = cleanString(words[0]).Trim().Replace(' ', '-');
                        foreach (string keyword in line.Trim().Split(' '))
                        {
                            if (!jobs.ContainsKey(keyword))
                            {
                                if (!keywords.ContainsKey(keyword))
                                    keywords.Add(keyword, new List<string>());
                                if (!keywords[keyword].Contains(keywordItem))
                                    keywords[keyword].Add(keywordItem);
                            }
                            else
                            {
                                jobs[keyword].Add(keywordItem);
                            }


                        }
                    }

                } while (line != null);
            }

            



            foreach (KeyValuePair<string, List<string>> job in jobs)
            {
                if (!string.IsNullOrEmpty(job.Key))
                {
                    string JoinedKeywords = string.Join(" ", job.Value.Cast<string>()
                                 .Where(c => !string.IsNullOrWhiteSpace(c))
                                 .Distinct());
                    filtered += job.Key + " : " + cleanString(JoinedKeywords) + Environment.NewLine;
                }

            }

            foreach (KeyValuePair<string, List<string>> keyword in keywords)
            {
                if (!string.IsNullOrEmpty(keyword.Key))
                {
                    string JoinedKeywords = string.Join(" ", keyword.Value.Cast<string>()
                                 .Where(c => !string.IsNullOrWhiteSpace(c))
                                 .Distinct());
                    filtered += keyword.Key + " : " + cleanString(JoinedKeywords) + Environment.NewLine;
                }

            }

            return filtered;

        }


        /// <summary>
        /// Reducer function that combines data generated by the mapping crawlers
        /// </summary>
        /// <param name="mapStoragePath"></param>
        public static void ReduceJobs(string mapStoragePath, string candidatesStoragePath)
        {
            try
            {
                //Combine Mapping data in one folder
                string dataFile = "";
                //Loop Mapping output
                foreach (string file in Directory.EnumerateFiles(mapStoragePath, "*.txt"))
                {
                    dataFile += File.ReadAllText(file) + Environment.NewLine;
                    //Delete mapping information no longer required
                    File.Delete(file);
                }

                //Loop Candidates output
                foreach (string file in Directory.EnumerateFiles(candidatesStoragePath, "*.txt"))
                {
                    dataFile += File.ReadAllText(file) + Environment.NewLine;
                }

                dataFile = filterBySkills(dataFile);
                //Remove double spaces
                dataFile = dataFile.Replace("  ", " ");
                //Generate the final ouput to be used by hadoop mappers
                System.IO.File.WriteAllText(mapStoragePath + @"/CombinedFile.txt", dataFile);
            }

            catch (Exception e)
            {
                Console.WriteLine("Thread Abort Exception");
            }
            finally
            {
                Console.WriteLine("Data Has been reduced Completed Successfully");
            }

        }

        /// <summary>
        /// Helper function that removes extra spaces and new lines from string
        /// </summary>
        /// <param name="dirtyString"></param>
        /// <returns></returns>
        public static string cleanString(string dirtyString)
        {
            string cleanText = dirtyString;
            cleanText = dirtyString.Replace("\r\n", "");
            cleanText = dirtyString.Replace(":", "");
            cleanText = System.Text.RegularExpressions.Regex.Replace(dirtyString, @"\s+", " ");
            return cleanText;
        }

        public static string RemoveSpecialCharacters(string str)
        {
            str = str.Replace(",", "");
            str = str.Replace(".", "");
            return str;
        }

        /// <summary>
        /// Helper function that parses the style attribute and returns the rating value
        /// </summary>
        /// <param name="styleAttribute"></param>
        /// <returns></returns>
        public static int getRating(string styleAttribute)
        {
            string ratingString = "";
            int rating = 0;
            if (styleAttribute == "0")
                return rating;
            else
            {
                if (styleAttribute.Length > 0)
                {
                    ratingString = styleAttribute.Substring(styleAttribute.IndexOf("width:") + 6, 3);
                    Int32.TryParse(ratingString, out rating);
                }
            }
            return rating;
        }
    }
}
