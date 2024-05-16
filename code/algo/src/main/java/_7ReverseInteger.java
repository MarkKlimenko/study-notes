import static org.junit.jupiter.api.Assertions.assertEquals;

class _7ReverseInteger {
    public static void main(String[] args) {
        assertEquals(2143847412, execute(2147483412));
    }

    private static int execute(int x) {
        int store = 0;

        while (x != 0) {
            int pop = x % 10;
            x = x / 10;

            if (store > Integer.MAX_VALUE / 10 ||
                            (store == Integer.MAX_VALUE / 10 && pop > 7)
            ) return 0;
            if (store < Integer.MIN_VALUE / 10 ||
                            (store == Integer.MIN_VALUE / 10 && pop < -8)
            ) return 0;

            store = store * 10 + pop;
        }

        return store;
    }
}
