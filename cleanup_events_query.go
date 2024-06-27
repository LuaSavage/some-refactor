package history

import (
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"

	"gitlab.2gis.ru/ugc/achieves/src/events/bss"
)

// BuildSelectLaunchWithDuplicatedRegions возвращает группы дублей событий запуска ранее переданной даты, произошедших в одном регионе.
func BuildSelectLaunchWithDuplicatedRegions(beforeTime time.Time) (string, []interface{}, error) {
	return SelectEvents(NewBSSEventMixin(), "user_id", "subject_id", "array_agg(id)").
		Where(sq.Lt{"created_at": beforeTime.Format("2006-01-02")}).
		ByActionType(bss.Launch.String()).
		Where(sq.NotEq{"subject_id": ""}).
		GroupBy("user_id", "subject_id").
		Having("array_length(array_agg(id), 1) > 1").
		ToPGSql()
}

// BuildSelectAllExpired возвращает события старше конкретной даты
func BuildSelectAllExpired(beforeTime time.Time, batchSize uint64) (string, []interface{}, error) {
	return SelectEvents(NewBSSEventMixin(), "id").
		Where(sq.Lt{"created_at": beforeTime.Format("2006-01-02")}).
		OrderBy("created_at").
		Limit(batchSize).
		ToSql()
}

func BuildSelectOldNavigationCloseEvents(beforeTime time.Time, batchSize uint64) (string, []interface{}, error) {
	return SelectEvents(NewBSSEventMixin(), "id").
		ByActionType(bss.ActionNavigationClose.String()).
		Where(sq.Lt{
			"created_at": beforeTime.Format("2006-01-02"),
		}).
		Limit(batchSize).
		ToSql()
}

// BuildSelectLaunchWithoutRegion возвращает события запуска ранее текущего месяца, которые не относятся к конкретному региону.
func BuildSelectLaunchWithoutRegion(beforeTime time.Time, batchSize uint64) (string, []interface{}, error) {
	return SelectEvents(NewBSSEventMixin(), "id").
		ByActionType(bss.Launch.String()).
		Where(sq.Lt{"created_at": beforeTime.Format("2006-01-02")}).
		BySubjectID("").
		Limit(batchSize).
		ToSql()
}

// BuildSelectLaunchWithDuplicatedDates возвращает запрос для получения неуникальных событий запуска в текущем месяце,
// произошедших в один день в одном регионе.
// События могут влиять на награды "Новичок" и "Тирист", но для 1й нужна уникализация по дням, а для 2й - по регионам.
func BuildSelectLaunchWithDuplicatedDates() (string, []interface{}, error) {
	return SelectEvents(
		NewBSSEventMixin(),
		"user_id", "subject_id", "date_trunc('day', created_at)", "array_agg(id)",
	).InCurrentMonth().
		ByActionType(bss.Launch.String()).
		Where(sq.NotEq{"subject_id": ""}).
		GroupBy("user_id", "subject_id", "date_trunc('day', created_at)").
		Having("array_length(array_agg(id), 1) > 1").
		ToPGSql()
}

func BuildDeleteEventsByIDsQuery(
	cleanBefore time.Time,
	idQuery string,
	idParams ...interface{},
) (string, []interface{}, error) {
	return sq.Delete("event").
		Where(fmt.Sprintf("id IN (%s)", idQuery), idParams...).
		Where(sq.Eq{"source_int": NewBSSEventMixin().SourceInt()}).
		Where(sq.Lt{"created_at": cleanBefore.Format("2006-01-02")}).
		PlaceholderFormat(sq.Dollar).
		ToSql()
}

func BuildDeleteEventsByIDs(
	ids []int64,
	eventTime time.Time,
) (string, []interface{}, error) {
	return sq.Delete("event").
		Where(sq.Eq{"id": ids, "source_int": NewBSSEventMixin().SourceInt()}).
		Where(sq.Lt{"created_at": eventTime.Format("2006-01-02")}).
		PlaceholderFormat(sq.Dollar).
		ToSql()
}
