#include "revert_string.h"
#include "string.h"

void RevertString(char *str)
{
	int length = strlen(str) - 1;
	char t;
	int i = 0;
	while (i < length) {
		t = str[i];
		str[i] = str[length];
		str[length] = t;
		i++;
		length--;
	}
}

