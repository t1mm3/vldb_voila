#include "runtime_utils.hpp"
#include "runtime.hpp"

void
IResetable::reset()
{
	ASSERT(false && "Not implemented");
}


void
IResetableList::reset() {
	for (auto& s : resetables) {
		s->reset();
	}

}

void
IResetableList::add_resetable(IResetable* r)
{
	ASSERT(r);
	resetables.push_back(r);
}



void
runtime_partition(size_t* RESTRICT dest_counts, char** RESTRICT dest_data,
	size_t num_parts, char* RESTRICT data, size_t width, u64* hashes,
	size_t num, size_t offset)
{
#define A(PARTITIONS) \
		for (size_t i=0; i<num; i++) { \
			const u64 hash = hashes[i]; \
			const u64 p = hash % num_parts; \
			/* printf("copy value %lld to partition %lld (hash %llu)\n", */ \
			/*	(long long)i, (long long)p, (unsigned long long)hash); */ \
			dest_counts[p] ++; \
			char* src = data + (i + offset) * width; \
			char* dest = dest_data[p]; \
			memcpy(dest, src, width); \
			dest_data[p] += width; \
		}

#define B(PARTITIONS) case PARTITIONS: A(PARTITIONS); break;

	switch (num_parts) {
	case 0:
		ASSERT(false);
		break;

B(1)
B(2)
B(3)
B(4)
B(5)
B(6)
B(7)
B(8)
B(9)
B(10)
B(11)
B(12)
B(13)
B(14)
B(15)
B(16)
B(17)
B(18)
B(19)
B(20)
B(21)
B(22)
B(23)
B(24)
B(25)
B(26)
B(27)
B(28)
B(29)
B(30)
B(31)
B(32)
B(33)
B(34)
B(35)
B(36)
B(37)
B(38)
B(39)
B(40)
B(41)
B(42)
B(43)
B(44)
B(45)
B(46)
B(47)
B(48)
B(49)
B(50)
B(51)
B(52)
B(53)
B(54)
B(55)
B(56)
B(57)
B(58)
B(59)
B(60)
B(61)
B(62)
B(63)
B(64)
B(65)
B(66)
B(67)
B(68)
B(69)
B(70)
B(71)
B(72)
B(73)
B(74)
B(75)
B(76)
B(77)
B(78)
B(79)
B(80)
B(81)
B(82)
B(83)
B(84)
B(85)
B(86)
B(87)
B(88)
B(89)
B(90)
B(91)
B(92)
B(93)
B(94)
B(95)
B(96)
B(97)
B(98)
B(99)
B(100)
B(101)
B(102)
B(103)
B(104)
B(105)
B(106)
B(107)
B(108)
B(109)
B(110)
B(111)
B(112)
B(113)
B(114)
B(115)
B(116)
B(117)
B(118)
B(119)
B(120)
B(121)
B(122)
B(123)
B(124)
B(125)
B(126)
B(127)
B(128)
B(129)
B(130)
B(131)
B(132)
B(133)
B(134)
B(135)
B(136)
B(137)
B(138)
B(139)
B(140)
B(141)
B(142)
B(143)
B(144)
B(145)
B(146)
B(147)
B(148)
B(149)
B(150)
B(151)
B(152)
B(153)
B(154)
B(155)
B(156)
B(157)
B(158)
B(159)
B(160)
B(161)
B(162)
B(163)
B(164)
B(165)
B(166)
B(167)
B(168)
B(169)
B(170)
B(171)
B(172)
B(173)
B(174)
B(175)
B(176)
B(177)
B(178)
B(179)
B(180)
B(181)
B(182)
B(183)
B(184)
B(185)
B(186)
B(187)
B(188)
B(189)
B(190)
B(191)
B(192)
B(193)
B(194)
B(195)
B(196)
B(197)
B(198)
B(199)
B(200)

	default:
		A(num_parts);
		break;
	} 

#undef A
#undef B
}
