package com.github.sftwnd.oracle.plsql.wrap;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.attribute.FileTime;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Main {

    private static final List<String> excludeSchemas = new ArrayList<>();
    private static final List<String> schemas = new ArrayList<>();
    private static final List<String> schemaPatterns = new ArrayList<>();
    private static final List<String> objects = new ArrayList<>();
    private static final List<String> objectPatterns = new ArrayList<>();

    public static boolean filterSchema(String schemaName) {
        Objects.requireNonNull(schemaName);
        if (excludeSchemas.stream().map(schemaName::equals).filter(Boolean::booleanValue).findFirst().orElse(false)) {
            return false;
        }
        if (schemas.isEmpty() && schemaPatterns.isEmpty()) {
            return true;
        }
        if (schemas.stream().filter(schemaName::equals).map(ignore -> true).findFirst().orElse(false)) {
            return true;
        }
        return schemaPatterns.stream().filter(schemaName::matches).map(ignore -> true).findFirst().orElse(false);
    }

    public static boolean filterObjects(String objectName) {
        Objects.requireNonNull(objectName);
        if (objects.isEmpty() && objectPatterns.isEmpty()) {
            return true;
        }
        if (objects.stream().filter(objectName::equals).map(ignore -> true).findFirst().orElse(false)) {
            return true;
        }
        return objectPatterns.stream().filter(objectName::matches).map(ignore -> true).findFirst().orElse(false);
    }

    public static void main(String[] args) throws SQLException, IOException, ParseException {

        Option excludeSchemaOption = new Option("e", "exclude-schema", true, "exclude schema");
        Option schemaOption = new Option("s", "schema", true, "include schema");
        Option schemaPatternOption = new Option("p", "schema-pattern", true, "include schema filter by pattern");
        Option objectOption = new Option("o", "object", true, "include object");
        Option objectPatternOption = new Option("t", "object-pattern", true, "include object filter by pattern");

        Option defaultFileOption = new Option("d", "default-file", false, "use default file archive name");
        Option fileOption = new Option("f", "file", true, "zip file archive name");
        OptionGroup fileNameOptionGroup = new OptionGroup().addOption(fileOption).addOption(defaultFileOption);

        Option helpOption = new Option("h", "help", false, "print help message");

        Options options = new Options()
                .addOption(excludeSchemaOption)
                .addOption(schemaOption)
                .addOption(schemaPatternOption)
                .addOption(objectOption)
                .addOption(objectPatternOption)
                .addOptionGroup(fileNameOptionGroup)
                .addOption(helpOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption(helpOption)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("extract-source", options);
            return;
        }

        Optional.ofNullable(excludeSchemaOption.getValuesList())
                .stream()
                .flatMap(Collection::stream)
                .forEach(excludeSchemas::add);
        if (excludeSchemas.isEmpty()) {
            excludeSchemas.add("SYS");
            excludeSchemas.add("SYSTEM");
            excludeSchemas.add("CTXSYS");
            excludeSchemas.add("DVSYS");
            excludeSchemas.add("PUBLIC");
        }
        Optional.ofNullable(cmd.getOptionValues(excludeSchemaOption))
                .map(Arrays::asList)
                .stream()
                .flatMap(Collection::stream)
                .forEach(excludeSchemas::add);
        Optional.ofNullable(cmd.getOptionValues(schemaOption))
                .map(Arrays::asList)
                .stream()
                .flatMap(Collection::stream)
                .peek(excludeSchemas::remove)
                .forEach(schemas::add);
        Optional.ofNullable(cmd.getOptionValues(schemaPatternOption))
                .map(Arrays::asList)
                .stream()
                .flatMap(Collection::stream)
                .forEach(schemaPatterns::add);
        Optional.ofNullable(cmd.getOptionValues(objectOption))
                .map(Arrays::asList)
                .stream()
                .flatMap(Collection::stream)
                .forEach(objects::add);
        Optional.ofNullable(cmd.getOptionValues(objectPatternOption))
                .map(Arrays::asList)
                .stream()
                .flatMap(Collection::stream)
                .forEach(objectPatterns::add);

        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
        pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        pds.setURL("jdbc:oracle:thin:scart_load_p/password@srv8-longworelo.net.billing.ru:1521:ORADB");
        pds.setInitialPoolSize(1);
        pds.setMinPoolSize(1);
        pds.setMaxPoolSize(2);

        OutputStream os = System.out;
        String rootEntryName = "sources";
        if (cmd.hasOption(defaultFileOption) || (cmd.hasOption(fileOption) && cmd.getOptionValue(fileOption).isBlank())) {
            try (Connection conn = pds.getConnection();
                 PreparedStatement psttm = conn.prepareStatement("select global_name from global_name");
                 ResultSet rset = psttm.executeQuery())
            {
                String globalDatabaseName;
                rset.next();
                globalDatabaseName = rset.getString(1).toLowerCase();
                rootEntryName = globalDatabaseName.toLowerCase();
                os = new FileOutputStream(rootEntryName + ".zip");
            }
        } else if (cmd.hasOption(fileOption)) {
            String fileName = cmd.getOptionValue(fileOption);
            if (fileName.toLowerCase().endsWith(".zip")) {
                rootEntryName = fileName.substring(0, fileName.length()-4);
            }
            os = new FileOutputStream(fileName);
        }

        OracleSourceLoader source = new OracleSourceLoader(pds::getConnection);

        try (ZipOutputStream zos = new ZipOutputStream(os)) {
            final String _rootEntryName = rootEntryName;
            source.processSource((owner, sourceType, wrapStatus, name, text, createTime, lastDdlTime) -> {

                if (!filterSchema(owner)) {
                    System.err.println("Skip " +
                            sourceType.toString().toLowerCase().replace('_',' ') +
                            ' ' + owner + '.' + name + " by  theschema name filter"
                    );
                    return;
                }
                if (!filterObjects(name)) {
                    System.err.println("Skip " +
                            sourceType.toString().toLowerCase().replace('_',' ') +
                            ' ' + owner + '.' + name + " by the object name filter"
                    );
                    return;
                }

                System.out.println(sourceType + " " + owner+"."+name+" [wrapStatus: " + wrapStatus + "]");
                String entryName = _rootEntryName+"/"
                        + owner.toLowerCase()+"/"
                        + (wrapStatus.isWrapped() ? String.valueOf(wrapStatus).toLowerCase() + "/" : "")
                        + name.toLowerCase().replace("/","_");
                if (!wrapStatus.isWrapped()) {
                    switch (sourceType) {
                        case PACKAGE: entryName += "_pack.sql"; break;
                        case PACKAGE_BODY: entryName += "_pbod.sql"; break;
                        case TYPE: entryName += "_type.sql"; break;
                        case TYPE_BODY: entryName += "_tbod.sql"; break;
                        case PROCEDURE: entryName += "_proc.sql"; break;
                        case FUNCTION: entryName += "_func.sql"; break;
                        case TRIGGER: entryName += "_trig.sql"; break;
                        case JAVA_SOURCE: entryName += "_java.sql"; break;
                        case LIBRARY: entryName += "_lib.sql"; break;
                        default: entryName += "_" + sourceType.toString().toLowerCase() +".sql"; break;
                    }
                } else {
                    switch (sourceType) {
                        case PACKAGE: entryName += "_pack.pls"; break;
                        case PACKAGE_BODY: entryName += "_pbod.pls"; break;
                        case TYPE: entryName += "_type.pls"; break;
                        case TYPE_BODY: entryName += "_tbod.pls"; break;
                        case PROCEDURE: entryName += "_proc.pls"; break;
                        case FUNCTION: entryName += "_func.pls"; break;
                        case TRIGGER: entryName += "_trig.pls"; break;
                        case JAVA_SOURCE: entryName += "_java.javac"; break;
                        case LIBRARY: entryName += "_lib.lbb"; break;
                        default: entryName += "_" + sourceType.toString().toLowerCase() +".wrp"; break;
                    }
                }
                text = text.replace("\r\n", "\n").trim();
                if (text.charAt(text.length()-1) == '/') {
                    text += "\n";
                } else if (text.charAt(text.length()-1) != '\n') {
                    text += "\n/\n";
                } else {
                    text += "/\n";
                }
                byte[] buff = text.getBytes();
                ZipEntry zipEntry = new ZipEntry(entryName);
                Optional.ofNullable(createTime)
                        .map(FileTime::from)
                        .ifPresent(zipEntry::setCreationTime);
                Optional.ofNullable(lastDdlTime)
                        .map(FileTime::from)
                        .ifPresent(zipEntry::setLastModifiedTime);
                zos.putNextEntry(zipEntry);
                zos.write(CREATE_OR_REPLACE);
                zos.write(buff);
                zos.closeEntry();
            }, schemaPatterns.isEmpty() ? schemas : null, excludeSchemas);
        }
    }

    private static final byte[] CREATE_OR_REPLACE = "CREATE OR REPLACE ".getBytes();

}
