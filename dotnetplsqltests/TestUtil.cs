using System;
using System.Collections.Generic;
using Oracle.DataAccess.Client;
using spinat.dotnetplsql;
using System.Text;

namespace spinat.dotnetplsqltests {

public class TestUtil {

    //public static Properties getProperties(String key) {
    //    try {
    //        Class cl = TestUtil.class;
    //        ClassLoader l = cl.getClassLoader();
    //        InputStream ins = l.getResourceAsStream(key);
    //        Properties props = new java.util.Properties();
    //        props.load(ins);
    //        return props;
    //    } catch (IOException ex) {
    //        throw new RuntimeException("fail", ex);
    //    }
    //}

    public static Dictionary<String, String> LoadSnippets(String name) {
        System.IO.StreamReader file = new System.IO.StreamReader(name);
        var res = new Dictionary<String, String>();
        String line;
        while (true) {
            line = file.ReadLine();
            if (line == null || line.StartsWith("##")) {
                break;
            }
        }
        while (true) {
            if (line == null) {
                return res;
            }
            String key = line.Substring(3);
            key = key.Trim();
            StringBuilder sb = new StringBuilder();
            while (true) {
                line = file.ReadLine();
                if (line == null || line.StartsWith("##")) {
                    res[key] = sb.ToString();
                    break;
                } else {
                    sb.Append(line);
                    sb.Append("\n");
                }
            }
        }
    }

}
}
