package com.github.sftwnd.oracle.plsql.wrap;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static com.github.sftwnd.oracle.plsql.wrap.OracleSourceLoader.WrapStatus.NonWrapped;
import static com.github.sftwnd.oracle.plsql.wrap.OracleSourceLoader.WrapStatus.UnWrapped;
import static com.github.sftwnd.oracle.plsql.wrap.OracleSourceLoader.WrapStatus.Wrapped;
import static com.github.sftwnd.oracle.plsql.wrap.OracleSourceLoader.WrapStatus.Wrapped920;

public class OracleSourceLoader {

    public enum WrapStatus {
        NonWrapped(false),
        UnWrapped(false),
        Wrapped(true),
        Wrapped920(true);

        final boolean wrapped;
        WrapStatus(boolean wrapped) {
            this.wrapped = wrapped;
        }

        public boolean isWrapped() {
            return wrapped;
        }

    }
    public enum SourceType {
        PACKAGE,
        PACKAGE_BODY,
        TYPE,
        TYPE_BODY,
        PROCEDURE,
        FUNCTION,
        TRIGGER,
        JAVA_SOURCE,
        LIBRARY
    }
    Connector connector;
    @FunctionalInterface
    interface Connector {
        Connection getConnection() throws SQLException;
    }

    OracleSourceLoader(Connector connector) {
        this.connector = Objects.requireNonNull(connector, "OracleSourceLoader::new - connector is null");
    }

    private static final Charset WINDOWS1521 = Charset.forName("windows-1251");
    public void processSource(SourceListener listener) throws SQLException, IOException {
        processSource(listener,null);
    }

    public void processSource(SourceListener listener, List<String> schemas) throws SQLException, IOException {
        processSource(listener, schemas, null);
    }
    public void processSource(SourceListener listener, List<String> schemas, List<String> excludeSchemas) throws SQLException, IOException {
        processSource(listener, schemas, excludeSchemas, null);
    }

    void processSource(SourceListener listener,
                       List<String> schemas,
                       List<String> excludeSchemas,
                       List<String> objects
                       ) throws SQLException, IOException {
        Connection conn = connection();
        String query   = "select /*+ FIRST_ROWS(1) */\n" +
                         "       o.owner         as owner\n" +
                         "      ,o.object_type   as type\n" +
                         "      ,o.object_name   as name\n" +
                         "      ,s.text\n        as text\n" +
                         "      ,o.created       as create_time\n" +
                         "      ,o.last_ddl_time as last_ddl_time\n" +
                         "  from dba_objects o\n" +
                         "  join dba_source s\n" +
                         "    on s.owner = o.owner\n" +
                         "   and s.type = o.object_type\n" +
                         "   and s.name = o.object_name\n";
        String orderBy = " order by o.owner\n" +
                         "         ,o.object_type\n" +
                         "         ,o.object_name\n" +
                         "         ,s.line\n";
        String where = null;
        if (schemas != null && !schemas.isEmpty()) {
            where = "(o.owner in (";
            for (int i = 0; i < schemas.size(); i++) {
                if ( i > 0 ) where += ", ";
                where += '\'' + schemas.get(i) + '\'';
            }
            where += "))";
        }
        if (excludeSchemas != null && !excludeSchemas.isEmpty()) {
            String where1 = "(o.owner not in (";
            for (int i = 0; i < excludeSchemas.size(); i++) {
                if ( i > 0 ) where1 += ", ";
                where1 += '\'' + excludeSchemas.get(i) + '\'';
            }
            where1 += "))";
            where = where == null ? where1 : where + "\n      and " + where1;
        }if (objects != null && !objects.isEmpty()) {
            String where1 = "(o.object_name in (";
            for (int i = 0; i < objects.size(); i++) {
                if ( i > 0 ) where1 += ", ";
                where1 += '\'' + objects.get(i) + '\'';
            }
            where1 += "))";
            where = where == null ? where1 : where + "\n      and " + where1;
        }
        if (where != null) {
            query += "  where " + where + "\n";
        }
        try {
            String owner = null;
            SourceType type = null;
            String name = null;
            Instant createTime = null;
            Instant lastDdlTime = null;
            boolean wrapped = false;
            PreparedStatement psttm = conn.prepareStatement(query+orderBy);
            ResultSet rset = psttm.executeQuery();
            StringBuilder sb = null;
            while(rset.next()) {
                String new_owner = rset.getString(1);
                SourceType new_type = SourceType.valueOf(rset.getString(2).replace(' ', '_'));
                String new_name = rset.getString(3);
                String text = rset.getString(4);
                Timestamp new_createTime = rset.getTimestamp(5);
                Timestamp new_lastDdlTime = rset.getTimestamp(6);
                if (!new_name.equals(name) || new_type != type || !new_owner.equals(owner)) {
                    if (sb != null) {
                        WrapStatus wrapStatus = NonWrapped;
                        if (wrapped) {
                            try {
                                byte[] buff = UnWrapper.unwrap(sb.toString());
                                listener.source(owner, type, UnWrapped, name, new String(buff, WINDOWS1521), createTime, lastDdlTime);
                                wrapStatus = Wrapped;
                            } catch (Exception ex) {
                                wrapStatus = Wrapped920;
                            }
                        }
                        listener.source(owner, type, wrapStatus, name, sb.toString(), createTime, lastDdlTime);
                    }
                    sb = new StringBuilder();
                    owner = new_owner;
                    type = new_type;
                    name = new_name;
                    createTime = new_createTime.toInstant();
                    lastDdlTime = new_lastDdlTime.toInstant();
                    wrapped = text.toLowerCase().contains(" wrapped");
                }
                sb.append(text);
            }
            if (sb != null && sb.length() > 0) {
                if (sb.charAt(sb.length()-1) != '\n') {
                    sb.append('\n');
                }
                sb.append("/\n");
            }
        } finally {
            if (conn != null) conn.close();
        }
    }

    @FunctionalInterface
    public interface SourceListener {
        void source(String owner, SourceType sourceType, WrapStatus wrapStatus, String name, String text, Instant createTime, Instant lastDdlTime) throws IOException;
    }

    Connection connection() throws SQLException {
        return connector.getConnection();
    }

}
