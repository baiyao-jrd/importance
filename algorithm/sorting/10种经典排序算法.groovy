// 0. 校验排序算法正确性的小工具
/**
 * @Author: baiYao
 * @Date: 2023/10/01/10:32
 * @Description: 使用Arrays.sort(arr)校验排序算法的正确性

    思路很简单: new一个指定长度的数组, 随机赋值. 使用Arrays.sort(arr)排序
               复制随机数组, 使用咱们自己写的算法排下序, 最后比较每个位置元素是否相同, 一旦有一个位置不一样就说明写的不对

               循环上面过程3000次, 排除数组的偶然性
 */
public class SortChecker {
    public static void main(String[] args) {
        check(3000);
    }
    static int[] randomArray(int length) {
        Random random = new Random();

        int[] nums = new int[length];

        for (int i = 0; i < nums.length; i++) {
            nums[i] = random.nextInt(length);
        }

        return nums;
    }

    static void check(int loopTimes) {
        boolean isEqual = true;

        first: for (int times = 0; times < loopTimes; times++) {
            int[] numsA = randomArray(loopTimes);
            int[] numsB = new int[numsA.length];

            System.arraycopy(numsA, 0, numsB, 0, numsA.length);

            Arrays.sort(numsA);
            QuickSort.quickSort(0, numsB.length - 1, numsB);

            for (int i = 0; i < numsB.length; i++) {
                if (numsA[i] != numsB[i]) {
                    isEqual = false;

                    print(numsA);

                    break first;
                }

            }
        }
        if (isEqual) {
            System.out.println("正确");
        }
    }

    static void print(int[] arr) {
        for (int num : arr) {
            System.out.print(num + " ");
        }
    }
}


// 1. 冒泡排序
1.1 思路
假设n个元素, 那么就会有n-1轮冒泡

第一轮, 从头开始, 元素从前到后两两比较, 将最大的元素冒泡至最后一个位置
第二轮, 从头开始, 元素从前到后两两比较, 将最大的元素冒泡至倒数第二个位置
...
第n-1轮, 从头开始, 元素从前到后两两比较, 将最大的元素冒泡至倒数第n-1个位置, 也就是正数第二个位置

正序排序完成.

1.2 双循环
public static void bubbleSort(int[] intArr) {
    // 1. 一共需要比较 (元素总数-1) 趟
    for (int i = 0; i < intArr.length - 1; i++) {
        // 2. 对于每一趟, 要保证后一个元素 (j + 1) < lentgh, 同时考虑每一趟走完, 就会在末尾数排好一个位置, 
        //    那么下一趟, 排好的这个位置及以后的元素就不需要被比较了, 所以实际上得保证 (j + 1) < lentgh - i 也就是 j < intArr.length - i - 1
        for (int j = 0; j < intArr.length - i - 1; j++) {
            if (intArr[j + 1] < intArr[j]) {
                int temp = intArr[j + 1];
                intArr[j + 1] = intArr[j];
                intArr[j] = temp;
            }
        }
    }
}

1.3 递归 -> 递归效率没有双循环高
// 1. 需要给出两个参数: 数组, 长度
public static void bubbleSort(int[] intArr, int length) {
    // 2. 如果传入的长度是一个直接方法返回
    if (length < 2) return;

    // 3. 每一轮找出这一轮最大的放在最后面的位置
    //    代码中只需要保证第(i + 1)的元素的位置最大是length - 1
    for (int i = 0; i < length - 1; i++) {
        if (intArr[i + 1] < intArr[i]) {
            int temp = intArr[i + 1];
            intArr[i + 1] = intArr[i];
            intArr[i] = temp;
        }
    }

    // 4. 每一轮长度减一
    bubbleSort(intArr, --length);
}

1.4 优化 -> 1,2,3,4,5 本来就有序, 你排了一轮之后, 未交换任何两个元素之间的位置, 那么就提前返回

// 只要某一轮过后没有交换任何两位元素的位置, 说明前面的元素都已经正序了, 直接返回即可, 后续排序不需要再进行了

// 1.4.1 双循环优化版
public static void bubbleSort(int[] intArr) {
    for (int i = 0; i < intArr.length - 1; i++) {

        boolean ifSwap = false;

        for (int j = 0; j < intArr.length - i - 1; j++) {
            if (intArr[j + 1] < intArr[j]) {
                int temp = intArr[j + 1];
                intArr[j + 1] = intArr[j];
                intArr[j] = temp;

                ifSwap = true;
            }
        }

        if (!ifSwap) return;
    }
}

// 1.4.2 递归优化版
public static void bubbleSort(int[] intArr, int length) {
    if (length < 2) return;

    boolean ifSwap = false;

    for (int i = 0; i < length - 1; i++) {
        if (intArr[i + 1] < intArr[i]) {
            int temp = intArr[i + 1];
            intArr[i + 1] = intArr[i];
            intArr[i] = temp;

            ifSwap = true;
        }
    }

    if (!ifSwap) return;

    bubbleSort(intArr, --length);
}

// 5. 快速排序
5.1 思路

    ① i指针: 范围[start, end - 1], 是为了从左到右扫描出比中心轴位置的数小的值
    ② pivot指针: 指向中心轴位置
    ③ pivotIndex指针: 指向扩张位置; 由于所有小的都放左边, 所以pivotIndex用来指向当前被扫描到的小的数应该放的位置

    *
    *      i                    pivot
    *      ↓                    ↓
    *      5, 7, 1, 3, 2, 8, 6, 4
    *      ↑
    *      pivotIndex
    *

    *      -> i从左到右扫描, 发现1比4小, 所有把1放在pivotIndex的位置（也就是第一个位置, 交换位置元素）, pivotIndex索引指向下一个位置
    *            i              pivot
    *            ↓              ↓
    *      1, 7, 5, 3, 2, 8, 6, 4
    *         ↑
    *         pivotIndex
    *

    *      -> i继续往下扫描, 发现3比4小, 所有把3放在第二个位置, pivotIndex索引指向第三个位置
    *               i           pivot
    *               ↓           ↓
    *      1, 3, 5, 7, 2, 8, 6, 4
    *            ↑
    *            pivotIndex
    *

    *      -> i继续往下扫描, 发现2比4小, 所有把2放在第三个位置, pivotIndex索引指向第四个位置
    *                  i        pivot
    *                  ↓        ↓
    *      1, 3, 2, 7, 5, 8, 6, 4
    *               ↑
    *               pivotIndex
    *

    *      -> i继续往下扫描, 发现没有比4小的元素, 最后将pivot位置元素放在pivotIndex位置（交换元素位置）
    *                           pivot
    *                           ↓
    *      1, 3, 2, 4, 5, 8, 6, 7
    *               ↑
    *               pivotIndex
    *

    *      可以发现上面元素中pivotIndex位置确定后, 左侧(1, 3, 2)都比4小, 右侧(5, 8, 6, 7)都比4大


5.2 代码
public class QuickSort {
    public static void quickSort(int start, int end, int[] nums) {
        // 保证数组长度不为1的情况下, 找出中心轴位置, 继续分区排序
        if (start < end) {
            int pivot = partition(start, end, nums);
            quickSort(start, pivot - 1, nums);
            quickSort(pivot + 1, end, nums);
        }
    }

    public static int partition(int start, int end, int[] nums) {
        // 中心轴是最右的位置; pivotIndex是扩张位置, 当然最终指向的是中心轴位置
        int pivot = end;
        int pivotIndex = start;

        for (int i = start; i < end; i++) {
            if (nums[i] < nums[pivot]) {
                swap(nums, i, pivotIndex);
                pivotIndex++;
            }
        }
        swap(nums, pivotIndex, pivot);
        return pivotIndex;
    }

    public static void swap(int[] intArr, int indexA, int indexB) {
        int temp = intArr[indexB];
        intArr[indexB] = intArr[indexA];
        intArr[indexA] = temp;
    }
}
