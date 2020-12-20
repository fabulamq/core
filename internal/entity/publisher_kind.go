package entity

type PublisherKind string


func GetPublisherKind(k string) PublisherKind {
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
	Undefined       PublisherKind = "undefined"
	Unique          PublisherKind = "unique"
	FundHeadQuarter PublisherKind = "fund_headquarter"
	HeadQuarter     PublisherKind = "headquarter"
	Branch          PublisherKind = "branch"
)
