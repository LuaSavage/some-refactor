package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/alecthomas/kingpin"
	"gitlab.2gis.ru/ugc-go-libs/database/database"
	"gitlab.2gis.ru/ugc-go-libs/log/v2/log"
	"gitlab.2gis.ru/ugc-go-libs/signals/signals"

	"gitlab.2gis.ru/ugc/achieves/src/achieves/history"
	"gitlab.2gis.ru/ugc/achieves/src/events/bss"
)

// CleanUpBSSEventsCommand команда для чистки bss событий, больше не влияющих на рассчёты
type CleanUpBSSEventsCommand struct {
	logger log.Logger
	config CleanUpDbConfig
	db     *database.Database
	repo   *history.CleanUpBSSEventsRepo
}

// NewCleanupBSSEventsCommand создаёт команду для чистки bss событий, больше не влияющих на рассчёты
func NewCleanupBSSEventsCommand(
	config CleanUpDbConfig,
) *CleanUpBSSEventsCommand {
	return &CleanUpBSSEventsCommand{
		config: config,
		repo:   history.NewCleanUpBSSEventsRepo(),
	}
}

// InitArgs чтение параметров команды
func (c *CleanUpBSSEventsCommand) InitArgs(cmd *kingpin.CmdClause, flagAppender ...FlagsAppender) {
	for _, appender := range flagAppender {
		appender(cmd)
	}

	cmd.Flag("batch-timeout", "Задержка между обработкой пачек").
		Default("60s").
		DurationVar(&c.repo.BatchTimeout)

	cmd.Flag("batch-size", "Размер пачки на удаление").
		Default("200000").
		Uint64Var(&c.repo.BatchSize)
}

// Run обработчик запуска команды
func (c *CleanUpBSSEventsCommand) Run(_ *kingpin.ParseContext) (err error) {
	c.logger = log.New(c.config.Logger().Format, c.config.Logger().Level, os.Stdout)
	c.db, err = InitDb(c.config.DB().Dsn, c.config.DB().MaxOpenConns, c.logger)
	if err != nil {
		c.logger.WithError(err).Error("не удалось выполнить подключение к БД")
		return fmt.Errorf("подключение к БД: %w", err)
	}
	c.repo.Logger = c.logger
	c.repo.Db = c.db.DB()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error)
	signals.BindSignals(c.logger, errChan, func(code int) {
		cancel()
		errDB := c.db.Close()
		if errDB != nil {
			c.logger.WithError(err).Error("не удалось выполнить закрытие БД")
		}
		os.Exit(code)
	})

	c.logger.Infof("Начинаем чистку ненужных событий и логов")

	err = c.cleanUpNavigationClose(ctx)
	if err != nil {
		c.logger.WithError(err).Error("Ошибка чистки событий NavigationClose")
		return fmt.Errorf("чистка событий NavigationClose: %w", err)
	}

	err = c.cleanUpRouteSearchCompleted(ctx)
	if err != nil {
		c.logger.WithError(err).Error("Ошибка чистки событий RouteSearchCompleted")
		return fmt.Errorf("чистка событий RouteSearchCompleted: %w", err)
	}

	err = c.repo.CleanUpAllExpired(ctx)
	if err != nil {
		c.logger.WithError(err).Error("Ошибка чистки устаревших событий всех типов")
		return fmt.Errorf("чистка устаревших событий всех типов: %w", err)
	}

	err = c.cleanUpLaunch(ctx)
	if err != nil {
		c.logger.WithError(err).Error("Ошибка чистки событий Launch")
		return fmt.Errorf("чистка событий Launch: %w", err)
	}

	c.logger.Infof("Чистка успешно завершена")

	return nil
}

func (c *CleanUpBSSEventsCommand) cleanUpNavigationClose(ctx context.Context) error {
	return c.repo.CleanUpAllCompletedEvents(ctx, "bip_person", bss.ActionNavigationClose)
}

func (c *CleanUpBSSEventsCommand) cleanUpRouteSearchCompleted(ctx context.Context) error {
	return c.repo.CleanUpAllCompletedEvents(ctx, "pass_for_ride", bss.RouteSearchCompleted)
}

// Удаляет события запуска приложения, которые больше не влияют на вычисление наград, т.к. награда уже получена
func (c *CleanUpBSSEventsCommand) cleanUpLaunch(ctx context.Context) (err error) {
	actionLogger := c.logger.WithField("action_type", bss.Launch)

	actionLogger.Info("Чистим старые события без регионов")
	err = c.repo.CleanUpLaunchWithoutRegion(ctx, actionLogger)
	if err != nil {
		return fmt.Errorf("чистка событий Launch без региона: %w", err)
	}

	actionLogger.Info("Чистим дубли регионов в события прошлого месяца")
	err = c.repo.CleanUpLaunchWithDuplicatedRegions(ctx, actionLogger)
	if err != nil {
		return fmt.Errorf("чистка событий Launch с дублированием регионов: %w", err)
	}

	actionLogger.Info("Чистим дубли запусков в 1 день в 1 регионе в события текущего месяца")
	err = c.repo.CleanUpLaunchWithDuplicatedDates(ctx, actionLogger)
	if err != nil {
		return fmt.Errorf("чистка событий Launch с дублированием дат: %w", err)
	}

	actionLogger.Info("Чистим события навигации старше месяца")
	err = c.repo.CleanUpOldNavigationCloseEvents(ctx, actionLogger)
	if err != nil {
		return fmt.Errorf("чистка событий навигации старше месяца: %w", err)
	}

	return nil
}
