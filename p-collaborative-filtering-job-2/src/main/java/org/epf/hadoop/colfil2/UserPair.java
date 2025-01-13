import org.apache.hadoop.io.*;
import java.io.*;

public class UserPair implements WritableComparable<UserPair> {
    private String user1;
    private String user2;

    // Constructeur par défaut
    public UserPair() {}

    // Constructeur avec deux utilisateurs
    public UserPair(String user1, String user2) {
        if (user1.compareTo(user2) < 0) {
            this.user1 = user1;
            this.user2 = user2;
        } else {
            this.user1 = user2;
            this.user2 = user1;
        }
    }

    // Getters
    public String getUser1() {
        return user1;
    }

    public String getUser2() {
        return user2;
    }

    // Sérialisation de la paire d'utilisateurs
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(user1);
        out.writeUTF(user2);
    }

    // Désérialisation de la paire d'utilisateurs
    @Override
    public void readFields(DataInput in) throws IOException {
        this.user1 = in.readUTF();
        this.user2 = in.readUTF();
    }

    // Comparaison des paires d'utilisateurs (par ordre lexicographique)
    @Override
    public int compareTo(UserPair other) {
        int cmp = this.user1.compareTo(other.user1);
        if (cmp != 0) {
            return cmp;
        }
        return this.user2.compareTo(other.user2);
    }

    // Méthode toString pour afficher la paire d'utilisateurs
    @Override
    public String toString() {
        return user1 + "," + user2;
    }
}
