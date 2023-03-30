CREATE TABLE `tb_news` (
	`id`	INT	NOT NULL,
	`title`	VARCHAR(64)	NULL,
	`writed_at`	DATETIME	NULL,
	`content`	VARCHAR(64)	NULL,
	`author`	VARCHAR(64)	NULL,
	`flaform`	boolean	NULL,
	`main_category`	VARCHAR(64)	NULL,
	`sub_category`	VARCHAR(64)	NULL
);

CREATE TABLE `tb_comment` (
	`id`	INT	NOT NULL,
	`news_id`	INT	NOT NULL,
	`user_id`	VARCHAR(64)	NULL,
	`writed_at`	DATETIME	NULL,
	`content`	VARCHAR(64)	NULL
);

CREATE TABLE `tb_sticker` (
	`id`	INT	NOT NULL,
	`news_id`	INT	NOT NULL,
	`name`	VARCHAR(64)	NULL,
	`count`	INT	NULL
);

ALTER TABLE `tb_news` ADD CONSTRAINT `PK_TB_NEWS` PRIMARY KEY (
	`id`
);

ALTER TABLE `tb_comment` ADD CONSTRAINT `PK_TB_COMMENT` PRIMARY KEY (
	`id`
);

ALTER TABLE `tb_sticker` ADD CONSTRAINT `PK_TB_STICKER` PRIMARY KEY (
	`id`
);

