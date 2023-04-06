package example1.avro;

import example1.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import java.io.File;
import java.io.IOException;


public class UserApplication {

    public static void main(String[] args) throws IOException {
        // Alternate constructor
        User user2 = new User("Liza", 7, "red");
        // Construct via builder
        User user3 = User.newBuilder()
                .setName("Ross")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();


        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user2.getSchema(), new File("users.avro"));
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
        System.out.println("Data Serialized.");
    }


}
