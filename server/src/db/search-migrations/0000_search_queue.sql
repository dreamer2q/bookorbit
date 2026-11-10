CREATE SCHEMA "book_search";
--> statement-breakpoint
CREATE TABLE "book_search"."pending" (
	"book_id" integer PRIMARY KEY NOT NULL,
	"revision" bigint NOT NULL,
	"queued_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "pending_positive_ids" CHECK ("book_search"."pending"."book_id" > 0 AND "book_search"."pending"."revision" > 0)
);
--> statement-breakpoint
CREATE TABLE "book_search"."state" (
	"id" integer PRIMARY KEY DEFAULT 1 NOT NULL,
	"epoch" uuid DEFAULT gen_random_uuid() NOT NULL,
	"revision" bigint DEFAULT 0 NOT NULL,
	"applied_revision" bigint DEFAULT 0 NOT NULL,
	"snapshot_complete" boolean DEFAULT false NOT NULL,
	"write_coverage_complete" boolean DEFAULT false NOT NULL,
	CONSTRAINT "state_singleton" CHECK ("book_search"."state"."id" = 1),
	CONSTRAINT "state_revisions" CHECK ("book_search"."state"."applied_revision" >= 0 AND "book_search"."state"."applied_revision" <= "book_search"."state"."revision")
);
--> statement-breakpoint
CREATE INDEX "pending_revision_book_idx" ON "book_search"."pending" USING btree ("revision","book_id");