using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using System.Data;

namespace spinat.dotnetplslmanaged
{

    /*
    DBMS_UTILITY.NAME_RESOLVE (
    name          IN  VARCHAR2, 
    context       IN  NUMBER,
    schema        OUT VARCHAR2, 
    part1         OUT VARCHAR2, 
    part2         OUT VARCHAR2,
    dblink        OUT VARCHAR2, 
    part1_type    OUT NUMBER, 
    object_number OUT NUMBER);
    
    5 - synonym
    7 - procedure (top level)
    8 - function (top level)
    9 - package
    */
    class ResolvedName
    {

        public readonly String schema;
        public readonly String part1;
        public readonly String part2;
        public readonly String dblink;
        public readonly int part1_type;
        public readonly String object_number;

        public String toString()
        {
            return "" + schema + ", " + part1 + ", " + part2 + ", " + dblink + ", " + part1_type + ", " + object_number;
        }

        public ResolvedName(String schema,
                String part1,
                String part2,
                String dblink,
                int part1_type,
                String object_number)
        {
            this.schema = schema;
            this.part1 = part1;
            this.part2 = part2;
            this.dblink = dblink;
            this.part1_type = part1_type;
            this.object_number = object_number;
        }
    }

    class Resolver
    {
        private static String getStringParam(OracleCommand c, String name)
        {
            OracleParameter p = c.Parameters[name];
            if (Convert.IsDBNull(p.Value))
            {
                return null;
            }
            if (p.Value is string)
            {
                return (String)p.Value;
            }

            if (p.Value is Oracle.ManagedDataAccess.Types.OracleString)
            {
                var x = (Oracle.ManagedDataAccess.Types.OracleString)p.Value;
                if (x.IsNull)
                {
                    return null;
                }
                else
                {
                    return x.Value;
                }
            }
            throw new ApplicationException("failed paramter retrieve");
        }


        public static ResolvedName ResolveName(OracleConnection con, String name, bool typeContext)
        {
            using (OracleCommand cstm = con.CreateCommand())
            {
                cstm.CommandText = "begin dbms_utility.name_resolve(:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8);end;";
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P1";
                    p.DbType = DbType.String;
                    p.Value = name;
                    cstm.Parameters.Add(p);
                }

                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P2";
                    p.OracleDbType = OracleDbType.Int32;
                    if (typeContext)
                    {
                        p.Value = 7;
                    }
                    else
                    {
                        p.Value = 1; // is context PL/SQL code
                    }
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P3";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Direction = ParameterDirection.Output;
                    p.Size = 1000;
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P4";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Direction = ParameterDirection.Output;
                    p.Size = 1000;
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P5";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Direction = ParameterDirection.Output;
                    p.Size = 1000;
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P6";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Direction = ParameterDirection.Output;
                    p.Size = 1000;
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P7";
                    p.OracleDbType = OracleDbType.Int32;
                    p.Direction = ParameterDirection.Output;
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P8";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Size = 1000;
                    p.Direction = ParameterDirection.Output;
                    cstm.Parameters.Add(p);
                }
                cstm.ExecuteNonQuery();
                String schema = getStringParam(cstm, "P3");
                String part1 = getStringParam(cstm, "P4");
                String part2 = getStringParam(cstm, "P5");
                String dblink = getStringParam(cstm, "P6");
                int part1_type;
                if (cstm.Parameters["P7"].Value == null)
                {
                    part1_type = 0;
                }
                else
                {
                    var x = (Oracle.ManagedDataAccess.Types.OracleDecimal)cstm.Parameters["P7"].Value;
                    if (x.IsNull)
                    {
                        part1_type = 0;
                    }
                    else
                    {
                        part1_type = (int)x.Value;
                    }
                }
                String object_number = getStringParam(cstm, "P8");
                return new ResolvedName(schema, part1, part2, dblink, part1_type, object_number);
            }
        }

    
    }
}
