
using System;
using Oracle.DataAccess.Client;

namespace spinat.dotnetplsqltests
{
    class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Los gehts!");
            //ErrorTests();
            SysRefCursorTests();
            
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

        static void SysRefCursorTests()
        {
            var x = new FuncTestManaged();
            x.setUp();
            x.TestSysRefCursor();
            x.TestSysRefCursorAsDataTable();
            x.TestRefCursor();
            x.TestRefCursorAsDataTable();
        }
    }
}