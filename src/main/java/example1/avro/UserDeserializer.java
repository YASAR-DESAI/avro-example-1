package example1.avro;

import org.apache.avro.file.DataFileReader;
import java.io.File;
import java.io.IOException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

public class UserDeserializer {

    public static void main(String[] args) throws IOException {

        File file = new File("users.avro");
        // Deserialize Users from disk
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
// Reuse user object by passing it to next(). This saves us from
// allocating and garbage collecting many objects for files with
// many items.
            user = dataFileReader.next(user);
            System.out.println("Deserialized Data: \n"+user);
        }

    }
}
