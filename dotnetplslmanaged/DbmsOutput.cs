using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;


namespace spinat.dotnetplslmanaged
{
    public class DbmsOutput
    {

        public static List<String> FetchDbmsOutput(OracleConnection con)
        {
            /*        DBMS_OUTPUT.GET_LINES (
             lines       OUT     DBMSOUTPUT_LINESARRAY,
             numlines    IN OUT  INTEGER);*/
            using (OracleCommand cmd = con.CreateCommand())
            {

                cmd.CommandText = "declare a DBMSOUTPUT_LINESARRAY; begin dbms_output.get_lines(lines=> a,numlines=> :P1); open :P2 for select * from table(a);end;";
                var p1 = cmd.CreateParameter();
                p1.ParameterName = "P1";
                p1.Direction = ParameterDirection.InputOutput;
                p1.OracleDbType = OracleDbType.Int32;
                p1.Value = 256 * 256 * 256 * 127;
                cmd.Parameters.Add(p1);
                var p2 = cmd.CreateParameter();
                p2.ParameterName = "P2";
                p2.OracleDbType = OracleDbType.RefCursor;
                cmd.Parameters.Add(p2);

                cmd.ExecuteNonQuery();
                var cursor = (Oracle.ManagedDataAccess.Types.OracleRefCursor)p2.Value;
                var dr = cursor.GetDataReader();
                var res = new List<string>();
                while (dr.Read())
                {
                    if (dr.IsDBNull(0))
                    {
                        res.Add("");
                    }
                    else
                    {
                        res.Add(dr.GetString(0));
                    }
                }
                return res;
            }
        }
        public static void EnableDbmsOutput(OracleConnection con, int size)
        {
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "begin dbms_output.enable(buffer_size => :P1);end;";
                var p1 = cmd.CreateParameter();
                p1.ParameterName = "P1";
                p1.OracleDbType = OracleDbType.Int32;
                p1.Direction = ParameterDirection.Input;
                if (size >= 0)
                {
                    p1.Value = size;
                }
                else
                {
                    p1.Value = DBNull.Value;
                }
                cmd.Parameters.Add(p1);
                cmd.ExecuteNonQuery();
            }
        }

       
        public static void DisableDbmsOutput(OracleConnection con)
        {
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "begin dbms_output.disable;end;";
                cmd.ExecuteNonQuery();
            }
        }
    }
}
