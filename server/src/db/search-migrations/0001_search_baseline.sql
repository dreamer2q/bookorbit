CREATE TABLE "book_search"."baseline" (
	"id" integer PRIMARY KEY DEFAULT 1 NOT NULL,
	"epoch" uuid NOT NULL,
	"index_name" varchar(128) NOT NULL,
	"schema_revision" integer NOT NULL,
	"phase" text NOT NULL,
	"upper_book_id" integer NOT NULL,
	"cursor_book_id" integer DEFAULT 0 NOT NULL,
	"seeded_count" bigint DEFAULT 0 NOT NULL,
	"seed_revision" bigint NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "baseline_singleton" CHECK ("book_search"."baseline"."id" = 1),
	CONSTRAINT "baseline_range" CHECK ("book_search"."baseline"."cursor_book_id" >= 0 AND "book_search"."baseline"."cursor_book_id" <= "book_search"."baseline"."upper_book_id" AND "book_search"."baseline"."seeded_count" >= 0 AND "book_search"."baseline"."seed_revision" > 0),
	CONSTRAINT "baseline_phase" CHECK ("book_search"."baseline"."phase" IN ('seeding', 'catchup', 'indexed'))
);
--> statement-breakpoint
ALTER TABLE "book_search"."state" ADD COLUMN "index_name" varchar(128) DEFAULT '' NOT NULL;--> statement-breakpoint
ALTER TABLE "book_search"."state" ADD COLUMN "schema_revision" integer DEFAULT 0 NOT NULL;