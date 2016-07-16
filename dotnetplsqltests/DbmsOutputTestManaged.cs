using System;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using spinat.dotnetplslmanaged;
using NUnit.Framework;

namespace spinat.dotnetplsqltests
{


    public class DbmsOutputTestManaged
    {

        public DbmsOutputTestManaged()
        {
        }

        OracleConnection connection;

        [OneTimeSetUp]
        public void setUp()
        {

            var b = new OracleConnectionStringBuilder();
            b.UserID = "roland";
            b.Password = "roland";
            b.DataSource = "ltrav01";

            var c = new OracleConnection();
            c.ConnectionString = b.ConnectionString;
            c.Open();
            this.connection = c;
        }

        [Test]
        public void TestOutput()
        {

            DbmsOutput.DisableDbmsOutput(this.connection);
            DbmsOutput.EnableDbmsOutput(this.connection, 1000000);
            using (var cmd = this.connection.CreateCommand())
            {
                cmd.CommandText = "begin dbms_output.put_line('hallo'); dbms_output.put_line('');dbms_output.put_line('roland'); end;";
                cmd.ExecuteNonQuery();
            }
            var l = DbmsOutput.FetchDbmsOutput(this.connection);
            Assert.AreEqual(4, l.Count);
            Assert.AreEqual("hallo", l[0]);
            Assert.AreEqual("", l[1]);
            Assert.AreEqual("roland", l[2]);
        }

        [Test]
        public void TestOutputBig()
        {
            // this is slow with 1e6 line almost 30 sec.
            int n = 1000000;
            DbmsOutput.DisableDbmsOutput(this.connection);
            DbmsOutput.EnableDbmsOutput(this.connection, -1); // 1e6
            using (var cmd = this.connection.CreateCommand())
            {
                cmd.CommandText = "begin for i in 1 .. " + n + " loop dbms_output.put_line('this is line '||i);  end loop; end;";
                cmd.ExecuteNonQuery();
            }
            var l = DbmsOutput.FetchDbmsOutput(this.connection);
            Assert.AreEqual(n+1, l.Count);
            for (var i = 1; i <= n; i++)
            {
                Assert.AreEqual("this is line " + i, l[i-1]);
            }
        }
    }
}