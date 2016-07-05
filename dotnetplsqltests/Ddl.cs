using System;
using Oracle.DataAccess.Client;
namespace spinat.dotnetplsqltests
{


    public class Ddl
    {

        public static void createType(OracleConnection c, String s)
        {
            OracleCommand stm = c.CreateCommand();

            var a = s.Split(new String[] { " " }, StringSplitOptions.RemoveEmptyEntries);

            String create = a[0];
            String type = a[1];
            String name = a[2];

            try
            {
                stm.CommandText = "drop type " + name + " force";
                stm.ExecuteNonQuery();
            }
            catch (Exception x)
            {
                System.Console.WriteLine(x.Message);
            }

            stm.CommandText = s;
            stm.ExecuteNonQuery();
        }

        public static void call(OracleConnection con, params String[] s)
        {
            using (OracleCommand call = con.CreateCommand())
            {
                call.CommandText = String.Join("\n", s);
                call.ExecuteNonQuery();

            }
        }

        public static void dropSynonyms(OracleConnection con)
        {
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "select synonym_name from user_synonyms";
                var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    call(con, "drop synonym " + r.GetString(1));
                }
            }
        }

        public static void dropGrants(OracleConnection con)
        {
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "select grantee,table_name,privilege from USER_TAB_PRIVS_MADE";
                var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    Ddl.call(con, "revoke " + r.GetString(3) + " on " + r.GetString(2) + " from " + r.GetString(1));
                }
            }
        }
    }
}
