package history

//nolint:gci
import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"
	"gitlab.2gis.ru/ugc-go-libs/database/database"
	"gitlab.2gis.ru/ugc-go-libs/log/v2/log"
	"gitlab.2gis.ru/ugc/achieves/src/achieves"
	"gitlab.2gis.ru/ugc/achieves/src/events/bss"
)

const (
	maxManualBatchSize = 10000
	maxBSSBatchSize    = 100
)

// CleanUpBSSEventsRepo репозиторий для cleaner'а BSS событий
type CleanUpBSSEventsRepo struct {
	Db           database.DB
	Logger       log.Logger
	BatchTimeout time.Duration
	BatchSize    uint64
}

// NewCleanUpBSSEventsRepo создаёт репозиторий чистильщика ненужных событий
func NewCleanUpBSSEventsRepo() *CleanUpBSSEventsRepo {
	return &CleanUpBSSEventsRepo{}
}

// CleanUpLaunchWithoutRegion чистит события запуска ранее текущего месяца, которые не относятся к конкретному региону.
// Такие события могли повлиять только на награду "Новичок" ("Турист" опирается на наличие региона),
// а т.к. он считается только за события в текущем месяце, то более старые не пригодятся.
func (c *CleanUpBSSEventsRepo) CleanUpLaunchWithoutRegion(ctx context.Context, logger log.FieldLogger) error {
	now := time.Now().UTC()
	beforeTime := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	idQuery, idParams, err := BuildSelectLaunchWithoutRegion(beforeTime, c.BatchSize)

	if err != nil {
		return fmt.Errorf("построение запроса выбора событий для удаления: %w", err)
	}

	if err = c.cleanUpEventsAndLogsByEventIDsQuery(ctx, logger, beforeTime, idQuery, idParams...); err != nil {
		return fmt.Errorf("удаление событий запуска с subject_id = '' и старше месяца: %w", err)
	}

	return nil
}

// CleanUpLaunchWithDuplicatedDates уникализирует события запуска в текущем месяце,
// произошедшие в один день в одном регионе.
// События могут влиять на награды "Новичок" и "Тирист", но для 1й нужна уникализация по дням, а для 2й - по регионам.
func (c *CleanUpBSSEventsRepo) CleanUpLaunchWithDuplicatedDates(ctx context.Context, logger log.FieldLogger) error {
	sqlQuery, params, err := BuildSelectLaunchWithDuplicatedDates()
	if err != nil {
		return fmt.Errorf("построение запроса получения списка дублей событий в днях: %w", err)
	}

	logger.Debugf("%s %#v", sqlQuery, params)

	rows, err := c.Db.QueryContext(ctx, sqlQuery, params...)
	if err != nil {
		return fmt.Errorf("выполнение запроса получения списка дублей событий в днях: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logger.WithError(closeErr).Error("ошибка закрытия курсора")
		}
	}()

	var userID, subjectID string
	var day pq.NullTime
	var eventIDs pq.Int64Array

	batchEventIDs := make([]int64, 0)
	currentDate := time.Now().UTC()
	for rows.Next() {
		err = rows.Scan(&userID, &subjectID, &day, &eventIDs)
		if err != nil {
			return fmt.Errorf("сканирование строки с дублями: %w", err)
		}

		deleteLogger := logger.
			WithField("user_id", userID).
			WithField("subject_id", subjectID).
			WithField("date", day.Time.Format("2006-01-02"))

		eventIDsToDelete := eventIDs[1:]
		deleteLogger.Infof("Оставляем событие %d. Удаляем дублей: %d", eventIDs[0], len(eventIDsToDelete))

		batchEventIDs = append(batchEventIDs, eventIDsToDelete...)
		if len(batchEventIDs) >= maxManualBatchSize {
			err = c.DeleteEventsAndLogsByEventIDs(ctx, logger, currentDate, batchEventIDs)
			if err != nil {
				return fmt.Errorf("удаление событий по id: %w", err)
			}
			batchEventIDs = make([]int64, 0)
		}
	}

	if len(batchEventIDs) > 0 {
		err = c.DeleteEventsAndLogsByEventIDs(ctx, logger, currentDate, batchEventIDs)
		if err != nil {
			return fmt.Errorf("удаление событий по id: %w", err)
		}
	}

	return nil
}

func (c *CleanUpBSSEventsRepo) CleanUpOldNavigationCloseEvents(ctx context.Context, logger log.FieldLogger) error {
	actionLogger := logger.WithField("action_type", bss.ActionNavigationClose)
	actionLogger.Infof("Удаляем события NavigationClose старше месяца.")

	beforeTime := time.Now().UTC().AddDate(0, -1, 0)
	idQuery, idParams, err := BuildSelectOldNavigationCloseEvents(beforeTime, c.BatchSize)
	if err != nil {
		return fmt.Errorf("построение запроса событий NavigationClose старше месяца: %w", err)
	}

	return c.cleanUpEventsAndLogsByEventIDsQuery(ctx, actionLogger, beforeTime, idQuery, idParams...)
}

// CleanUpAllExpired чистит события всех типов старше двух месяцев.
// В рамках UGC-9128 сделаны доработки для туриста, которые позволяют перестать хранить bss-события старше 1 месяца
// (1 месяц оставлен для запаса)
func (c *CleanUpBSSEventsRepo) CleanUpAllExpired(ctx context.Context) error {
	cleanBefore := time.Now().UTC().AddDate(0, -2, 0)
	idQuery, idParams, err := BuildSelectAllExpired(cleanBefore, c.BatchSize)
	if err != nil {
		return fmt.Errorf("построение запроса выбора событий для удаления: %w", err)
	}

	err = c.cleanUpEventsAndLogsByEventIDsQuery(ctx, c.Logger, cleanBefore, idQuery, idParams...)
	if err != nil {
		return fmt.Errorf("удаление всех событий старше %s: %w", cleanBefore, err)
	}

	return nil
}

// CleanUpLaunchWithDuplicatedRegions уникализирует события запуска ранее текущего месяца, произошедшие в одном регионе.
// События уже не влияют на награду "Новичок", а для "Тириста" важны разные регионы.
func (c *CleanUpBSSEventsRepo) CleanUpLaunchWithDuplicatedRegions(
	ctx context.Context,
	logger log.FieldLogger,
) error {
	now := time.Now().UTC()
	beforeTime := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	sqlQuery, params, err := BuildSelectLaunchWithDuplicatedRegions(beforeTime)
	if err != nil {
		return fmt.Errorf("построение запроса получения списка дублей событий в регионах: %w", err)
	}

	logger.Debugf("%s %#v", sqlQuery, params)

	rows, err := c.Db.QueryContext(ctx, sqlQuery, params...)
	if err != nil {
		return fmt.Errorf("выполнение запроса получения списка дублей событий в регионах: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			c.Logger.WithError(closeErr).Error("ошибка закрытия курсора")
		}
	}()

	var userID, subjectID string
	var eventIDs pq.Int64Array

	batchEventIDs := make([]int64, 0)
	for rows.Next() {
		err = rows.Scan(&userID, &subjectID, &eventIDs)
		if err != nil {
			return fmt.Errorf("сканирование строки с дублями: %w", err)
		}

		deleteLogger := logger.
			WithField("user_id", userID).
			WithField("subject_id", subjectID)

		eventIDsToDelete := eventIDs[1:]
		deleteLogger.Infof("Оставляем событие %d. Удаляем дублей: %d", eventIDs[0], len(eventIDsToDelete))

		batchEventIDs = append(batchEventIDs, eventIDsToDelete...)

		if len(batchEventIDs) >= maxManualBatchSize {
			if err = c.DeleteEventsAndLogsByEventIDs(ctx, logger, beforeTime, batchEventIDs); err != nil {
				return fmt.Errorf("удаление событий по id: %w", err)
			}
			batchEventIDs = make([]int64, 0)
		}
	}

	if len(batchEventIDs) > 0 {
		if err = c.DeleteEventsAndLogsByEventIDs(ctx, logger, beforeTime, batchEventIDs); err != nil {
			return fmt.Errorf("удаление событий по id: %w", err)
		}
	}

	return nil
}

// CleanUpAllCompletedEvents удаляет события относящиеся к bss наградам, которые уже выполнены пользователем.
// NOTE: Подходит только для наград с 1 заданием по bss
func (c *CleanUpBSSEventsRepo) CleanUpAllCompletedEvents(
	ctx context.Context,
	achieveID string,
	actionType bss.ActionType,
) error {
	achieveLogger := c.Logger.
		WithField("achieve_id", achieveID).
		WithField("action_type", actionType)

	achieveLogger.Infof("Удаляем события для полученных наград")

	deleteQuery := `
	with t1 as (
		select id, event.created_at from event, user_achieve
			where
				event.user_id = user_achieve.user_id
				and
				event.source_int = $1 and event.action_type = $2
				and
				user_achieve.achieve_id = $3 and user_achieve.status = $4
				limit $5
	) delete from event USING t1 where event.id = t1.id AND source_int = $1 AND event.created_at = t1.created_at
	`

	for {
		res, err := c.Db.ExecContext(
			ctx,
			deleteQuery,
			NewBSSEventMixin().SourceInt(),
			actionType.String(),
			achieveID,
			achieves.AchieveStatusCompleted,
			maxBSSBatchSize,
		)
		if err != nil {
			return fmt.Errorf("удаление событий, идентификатор %+v тип %+v: %+w", achieveID, actionType.String(), err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("удаление событий, получение результатов, идентификатор %+v тип %+v: %+w", achieveID, actionType.String(), err)
		}
		achieveLogger.Infof("Удалено %+v событий", rows)
		if rows < maxBSSBatchSize {
			break
		}
	}

	return nil
}

func (c *CleanUpBSSEventsRepo) DeleteEventsAndLogsByEventIDs(
	ctx context.Context,
	deleteLogger log.FieldLogger,
	beforeTime time.Time,
	eventIDsToDelete []int64,
) error {
	sqlQuery, params, err := BuildDeleteEventsByIDs(eventIDsToDelete, beforeTime)

	if err != nil {
		return fmt.Errorf("построение запроса удаления событий по id: %w", err)
	}

	deleteLogger.Debugf("%s %#v", sqlQuery, params)

	if _, err = c.Db.ExecContext(ctx, sqlQuery, params...); err != nil {
		return fmt.Errorf("выполнение запроса удаления событий по id: %w", err)
	}

	time.Sleep(c.BatchTimeout)

	return nil
}

func (c *CleanUpBSSEventsRepo) BatchExec(
	ctx context.Context,
	logger log.FieldLogger,
	sqlQuery string,
	params ...interface{},
) error {
	for {
		res, err := c.Db.ExecContext(ctx, sqlQuery, params...)
		if err != nil {
			return fmt.Errorf("выполнение запроса удаления событий: %w", err)
		}

		affectedRows, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("получение кол-ва удалённых событий: %w", err)
		}

		logger.Infof("Удалено записей: %d", affectedRows)

		if affectedRows < int64(c.BatchSize) {
			break
		}

		time.Sleep(c.BatchTimeout)
	}

	return nil
}

func (c *CleanUpBSSEventsRepo) cleanUpEventsAndLogsByEventIDsQuery(
	ctx context.Context,
	logger log.FieldLogger,
	cleanBefore time.Time,
	idQuery string,
	idParams ...interface{},
) error {
	sqlQuery, params, err := BuildDeleteEventsByIDsQuery(cleanBefore, idQuery, idParams...)
	if err != nil {
		return fmt.Errorf("построение запроса удаления событий: %w", err)
	}

	logger.Debugf("%s %#v", sqlQuery, params)

	if err = c.BatchExec(ctx, logger, sqlQuery, params...); err != nil {
		return fmt.Errorf("удаление событий: %w", err)
	}

	return nil
}
