package api

type publisherKind string


func GetPublisherKind(k string)publisherKind{
	switch k {
	case string(Undefined):
		return Undefined
	case string(HeadQuarter):
		return HeadQuarter
	case string(FundHeadQuarter):
		return FundHeadQuarter
	case string(Branch):
		return Branch
	case string(Unique):
		return Unique
	default:
		return Undefined
	}
}

const (
	Undefined       publisherKind = "undefined"
	Unique          publisherKind = "unique"
	FundHeadQuarter publisherKind = "fund_headquarter"
	HeadQuarter     publisherKind = "headquarter"
	Branch          publisherKind = "branch"
)
