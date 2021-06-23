class Solution {
    public int[] plusOne(int[] digits) {
        for (int i = digits.length - 1; i >= 0; i--) {
			if (digits[i] != 9) {
                digits[i]++;
				return digits;
			} 
			digits[i] = 0;
		}
                //跳出for循环，说明数字全部是9
		int[] temp = new int[digits.length + 1];
		temp[0] = 1;
		return temp;
    }
}