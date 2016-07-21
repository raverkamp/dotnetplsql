
using System;
using Oracle.DataAccess.Client;

namespace spinat.dotnetplsqltests
{
    class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Los gehts!");
            TableTest();
            
            Console.Write("Press any key to continue . . . ");
            Console.ReadKey(true);
        }

        static void ExampleTestManaged()
        {
            var b = new OracleConnectionStringBuilder();
            b.UserID = "roland";
            b.Password = "roland";
            b.DataSource = "ltrav01";

            var cm = new Oracle.ManagedDataAccess.Client.OracleConnection();
            cm.ConnectionString = b.ConnectionString;
            cm.Open();

            ExampleTestManaged tm = new ExampleTestManaged(cm);
            tm.example1();
        }

        static void FuncTest()
        {
            var ft = new FuncTest();
            ft.setUp();
            ft.testVarcharout();
            ft.testVarcharBig();
        }
        static void FuncTestManaged()
        {
            var ft = new FuncTestManaged();
            ft.setUp();
            ft.test10();
        }

        static void ErrorTests()
        {
            var et = new ErrorTestManaged();
            et.setUp();
            et.testRaiseApplicationError();
            et.testToLargeVarchar2();
            et.testParamMissing();
            et.testSlotMissing();

        }

        static void DbmsOutputTest()
        {
            var x = new DbmsOutputTestManaged();
            x.setUp();
            x.TestOutput();
            x.TestOutputBig();
        }

        static void TableTest()
        {
            var x = new FuncTestManaged();
            x.setUp();
           // x.test3WithTableBase(20);
            x.Bigrectest();
        }

        static void SysRefCursorTests()
        {
            var x = new FuncTestManaged();
            x.setUp();
          
            x.TestRefCursor4();
           
           
        }
    }
}