public class MergeSorted {
    public static void main(String[] args) {
        merge(
                new int[]{1, 2, 3, 0, 0, 0},
                3,
                new int[]{2, 5, 6},
                3
        );
    }

    public static void merge(int[] nums1, int m, int[] nums2, int n) {
        int resultPosition = 0;
        int[] result = new int[m + n];

        int mPosition = 0;
        int nPosition = 0;

        while (mPosition < m || nPosition < n) {
            if (nPosition >= n || nums1[mPosition] < nums2[nPosition] && mPosition < m) {
                result[resultPosition] = nums1[mPosition];
                mPosition++;
            } else {
                result[resultPosition] = nums2[nPosition];
                nPosition++;
            }

            resultPosition++;
        }

        for (int i = 0; i < m + n; i++) {
            nums1[i] = result[i];
        }
    }
}
