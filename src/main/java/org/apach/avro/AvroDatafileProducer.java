package org.apach.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroDatafileProducer {

    public void produce(final File destDir, final int numDatafiles, int recordsPerDatafile) throws IOException {
        final Schema schema = new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("user.avsc"));
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        int totalRecords = 0;
        for (int i=0; i < numDatafiles; i++) {
            System.out.println("Generating datafile #" + i);

            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                dataFileWriter.create(schema, new File(destDir, "datafile" + i + ".avro"));
                for (int j=0; j < recordsPerDatafile; j++) {
                    final GenericRecord user = new GenericData.Record(schema);
                    user.put("name", "name" + totalRecords);
                    user.put("id", totalRecords);
                    dataFileWriter.append(user);
                    totalRecords++;
                }
                dataFileWriter.flush();
            }
        }
    }

    public static void main(String[] args) {
        if (args == null || args.length != 3) {
            System.out.println("Usage: <DEST_DIR> <NUM_DATAFILES> <RECORDS_PER_DATAFILE>");
            System.exit(1);
        }

        final String dir = args[0];
        final int numDatafiles = Integer.parseInt(args[1]);
        final int recordsPerDatafile = Integer.parseInt(args[2]);

        final AvroDatafileProducer producer = new AvroDatafileProducer();
        try {
            producer.produce(new File(dir), numDatafiles, recordsPerDatafile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
