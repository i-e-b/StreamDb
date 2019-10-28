using System;
using System.IO;
using StreamDb;
// ReSharper disable PossibleNullReferenceException

namespace TracingApp
{
    class Program
    {
        // For memory tracing:
        //const int OVERWRITE_COUNT = 2;
        //const int DOCUMENT_COUNT = 15;

        // For call/performance tracing:
        const int OVERWRITE_COUNT = 10;
        const int DOCUMENT_COUNT = 100;


        static void Main()
        {
            Console.WriteLine("This is a simple app that stress-tests the StreamDB library.");
            Console.WriteLine("It's only intended for running analysis tools");

            
            using (var doc = MakeTestDocument())
            using (var ms = new MemoryStream())
            {
                var subject = Database_OLD.TryConnect(ms);

                Console.WriteLine($"Empty database is {ms.Length / 1024}kb");

                // write lots of documents, and overwrite them a lot of times
                for (int overwrites = 0; overwrites < OVERWRITE_COUNT; overwrites++)
                {
                    Console.Write("Writing a 100 document block");

                    for (int i = 0; i < DOCUMENT_COUNT; i++)
                    {
                        Console.Write(".");
                        doc.Seek(0, SeekOrigin.Begin);
                        subject.WriteDocument($"testdata-{i}", doc);
                    }

                    Console.WriteLine($"Done. Filled database is {(ms.Length / 1048576.0):#.00}MB");
                }
            }
        }
        
        private static Stream MakeTestDocument()
        {
            var ms = new MemoryStream();
            var rnd= new Random();
            var buf = new byte[1024];
            for (int i = 0; i < 150; i++)
            {
                rnd.NextBytes(buf);
                ms.Write(buf, 0, buf.Length);
            }
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }
    }
}
