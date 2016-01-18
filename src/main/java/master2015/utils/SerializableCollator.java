package master2015.utils;

import java.io.IOException;
import java.io.Serializable;
import java.text.Collator;
import java.util.Comparator;

public class SerializableCollator implements Comparator<String>, Serializable {
    private static final long serialVersionUID = 1L;
    private transient Collator collatorInstance;

    public SerializableCollator() {
        super();
        initCollatorInstance();
    }


    public int compare(final String o1, final String o2) {
        return collatorInstance.compare(o1, o2);
    }

    private void initCollatorInstance() {
        collatorInstance = Collator.getInstance();
    }

    private void writeObject(final java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    private void readObject(final java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initCollatorInstance();
    }
}

