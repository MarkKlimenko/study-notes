package core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ComparableComparator {

    List<User> users = new ArrayList<>();

    {
        Collections.sort(users);
    }

    private static class User implements Comparable<User> {
        @Override
        public int compareTo(User o) {
            return 0;
        }
    }
}
