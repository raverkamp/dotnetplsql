
using System;
using Oracle.DataAccess.Client;

namespace spinat.dotnetplsqltests
{
	class Program
	{
		public static void Main(string[] args)
		{
			Console.WriteLine("Hello World!");
			
			var b = new OracleConnectionStringBuilder();
			b.UserID = "roland";
			b.Password = "roland";
			b.DataSource = "ltrav01";
			
            //var c = new OracleConnection();
            //c.ConnectionString = b.ConnectionString;
            //c.Open();
			
            //ExampleTest t = new ExampleTest(c);
            //t.example1();

            var cm = new Oracle.ManagedDataAccess.Client.OracleConnection();
            cm.ConnectionString = b.ConnectionString;
            cm.Open();

            ExampleTestManaged tm = new ExampleTestManaged(cm);
            tm.example1();


			Console.Write("Press any key to continue . . . ");
			Console.ReadKey(true);
		}
	}
}