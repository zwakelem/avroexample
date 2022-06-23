package za.co.simplitate.avro.specific;


import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExample {

    public static void main(String[] args) throws IOException {

        Customer.Builder customerBuilder = Customer.newBuilder();
        customerBuilder.setAge(25);
        customerBuilder.setFirstName("Sibo");
        customerBuilder.setLastName("Mgabhi");
        customerBuilder.setHeight(120f);
        customerBuilder.setWeight(24f);
        customerBuilder.setAutomatedEmail(false);
        Customer customer = customerBuilder.build();

        System.out.println(customer);

        // write to file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        final DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
        dataFileWriter.append(customer);
        dataFileWriter.close();
        System.out.println("successfully wrote customerV1.avro");

        // read from file
        final File file = new File("customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        final DataFileReader<Customer> dataFileReader;
        System.out.println("Reading our specific record");
        dataFileReader = new DataFileReader<>(file, datumReader);
        while (dataFileReader.hasNext()) {
            Customer readCustomer = dataFileReader.next();
            System.out.println(readCustomer.toString());
            System.out.println("First name: " + readCustomer.getFirstName());
        }

    }
}
