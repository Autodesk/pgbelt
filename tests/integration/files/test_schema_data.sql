--
-- Name: fruits; Type: TABLE; Schema: public; Owner: owner
--

CREATE TABLE public.fruits (
    id integer,
    name character varying(20)
);


ALTER TABLE public.fruits OWNER TO owner;

--
-- Name: UsersCapital; Type: TABLE; Schema: public; Owner: owner
--

CREATE TABLE public."UsersCapital" (
    id bigint NOT NULL,
    hash_firstname text NOT NULL,
    hash_lastname text NOT NULL,
    gender character varying(6) NOT NULL,
    numericnan numeric(19,4), -- Testing for #571 Numeric NaN
    CONSTRAINT users_gender_check CHECK (((gender)::text = ANY (ARRAY[('male'::character varying)::text, ('female'::character varying)::text])))
);


ALTER TABLE public."UsersCapital" OWNER TO owner;

--
-- Name: UsersCapital2; Type: TABLE; Schema: public; Owner: owner
--

CREATE TABLE public."UsersCapital2" (
    id bigint NOT NULL,
    "hash_firstName" text NOT NULL,
    hash_lastname text NOT NULL,
    gender character varying(6) NOT NULL,
    CONSTRAINT users_gender_check CHECK (((gender)::text = ANY (ARRAY[('male'::character varying)::text, ('female'::character varying)::text])))
);


ALTER TABLE public."UsersCapital2" OWNER TO owner;

CREATE TABLE public.another_test_table (
    "someThingIDontKnow" uuid NOT NULL,
    "anotherThing" uuid NOT NULL
);

ALTER TABLE public.another_test_table OWNER TO owner;

--
-- Name: users_idx; Type: INDEX; Schema: public; Owner: owner
--

CREATE INDEX users_idx ON public."UsersCapital" (
    hash_firstname,
    hash_lastname
);

--
-- Name: users2_idx; Type: INDEX; Schema: public; Owner: owner
--

CREATE INDEX users2_idx ON public."UsersCapital" (
    hash_firstname,
    hash_lastname
);

-- Addressing the following index statement style: CREATE INDEX "existingEmailIds_email_id_idx" ON public."existingEmailIds" USING btree ("projectId", "emailId");
-- Issue #652
-- Did not add a primary key, helped iron out related quoting issues in the dump and load code.

CREATE TABLE public."existingSomethingIds" (
    "thingId" integer NOT NULL,
    "somethingId" character varying(255) NOT NULL
);

CREATE INDEX "existingSomethingIds_something_id_idx" ON public."existingSomethingIds" USING btree ("thingId", "somethingId");

--
-- Name: userS_id_seq; Type: SEQUENCE; Schema: public; Owner: owner
--

CREATE SEQUENCE public."userS_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public."userS_id_seq" OWNER TO owner;
ALTER SEQUENCE public."userS_id_seq" OWNED BY public."UsersCapital".id;
ALTER TABLE ONLY public."UsersCapital" ALTER COLUMN id SET DEFAULT nextval('public."userS_id_seq"'::regclass);

--
-- Name: users2_id_seq; Type: SEQUENCE; Schema: public; Owner: owner
-- NOTE: Intentionally NOT linked to a PK column (no OWNED BY / no DEFAULT nextval()).
-- This ensures we keep coverage for the non-PK sequence path, where values are
-- copied via dump + load (rather than being set from max(pk) on the destination).
--

CREATE SEQUENCE public.users2_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.users2_id_seq OWNER TO owner;

--
-- Data for Name: fruits; Type: TABLE DATA; Schema: public; Owner: owner
--
INSERT INTO public.fruits (id, name)
    VALUES (1, 'watermelon'),
    (2, 'pear'),
    (3, 'strawberry'),
    (4, 'grape');

--
-- Data for Name: UsersCapital; Type: TABLE DATA; Schema: public; Owner: owner
--

INSERT INTO public."UsersCapital" (id, hash_firstname, hash_lastname, gender, numericnan)
    VALUES (1, 'garbagefirst', 'garbagelast', 'male', 1),
    (2, 'garbagefirst1', 'garbagelast1', 'female', 0),
    (3, 'sdgarbagefirst', 'dgsadsrbagelast', 'male', 'NaN'),
    (4, 'dsdssdgarbagefirst', 'dgsaggggdjjjsrbagelast', 'female', 1),
    (5, 'dsdssdgarbagefirt', 'dgsagggdjjjsrbagelast', 'female', 0);


--
-- Data for Name: Users2; Type: TABLE DATA; Schema: public; Owner: owner
--

INSERT INTO public."UsersCapital2" (id, "hash_firstName", hash_lastname, gender)
    VALUES (1, 'garbagefirst', 'garbagelast', 'male'),
    (2, 'garbagefirst1', 'garbagelast1', 'female'),
    (3, 'sdgarbagefirst', 'dgsadsrbagelast', 'male'),
    (4, 'dsdssdgarbagefirst', 'dgsaggggdjjjsrbagelast', 'female'),
    (5, 'dsdssdgarbagefirt', 'dgsagggdjjjsrbagelast', 'female');


INSERT INTO public.another_test_table ("someThingIDontKnow", "anotherThing")
    VALUES ('0e095b60-ab7d-4892-9a92-6175497fe0f9', '0e095b60-ab7d-4892-9a92-6175497fe0f9');

--
-- Data for Name: existingSomethingIds; Type: TABLE DATA; Schema: public; Owner: owner
--

INSERT INTO public."existingSomethingIds" ("thingId", "somethingId")
    VALUES (1, 'something1'),
    (2, 'something2'),
    (3, 'something3'),
    (4, 'something4');


--
-- Name: userS_id_seq; Type: SEQUENCE SET; Schema: public; Owner: owner
--

SELECT pg_catalog.setval('public."userS_id_seq"', 16, false);


--
-- Name: users2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: owner
--

SELECT pg_catalog.setval('public.users2_id_seq', 15, false);


--
-- Name: UsersCapital users_pkey; Type: CONSTRAINT; Schema: public; Owner: owner
--

ALTER TABLE ONLY public."UsersCapital"
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: UsersCapital users_pkey; Type: CONSTRAINT; Schema: public; Owner: owner
--

ALTER TABLE ONLY public."UsersCapital2"
    ADD CONSTRAINT users2_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.another_test_table
    ADD CONSTRAINT another_test_table_pkey PRIMARY KEY ("someThingIDontKnow", "anotherThing");

--
-- Testing FK constraints that depend on unique indexes (not PKs or unique constraints).
-- This exercises the fix for the case where pgbelt defers a CREATE UNIQUE INDEX
-- that a FOREIGN KEY constraint depends on, causing the FK to fail at setup time.
--

CREATE TABLE public.fk_unique_parent (
    id integer NOT NULL,
    unique_code character varying(20) NOT NULL
);

ALTER TABLE public.fk_unique_parent OWNER TO owner;

CREATE TABLE public.fk_unique_child (
    id integer NOT NULL,
    parent_code character varying(20) NOT NULL
);

ALTER TABLE public.fk_unique_child OWNER TO owner;

INSERT INTO public.fk_unique_parent (id, unique_code) VALUES (1, 'code_a'), (2, 'code_b');
INSERT INTO public.fk_unique_child (id, parent_code) VALUES (1, 'code_a'), (2, 'code_b');

ALTER TABLE ONLY public.fk_unique_parent
    ADD CONSTRAINT fk_unique_parent_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.fk_unique_child
    ADD CONSTRAINT fk_unique_child_pkey PRIMARY KEY (id);

-- This is a UNIQUE INDEX (not a UNIQUE CONSTRAINT). The FK below depends on it.
CREATE UNIQUE INDEX fk_unique_parent_unique_code_idx ON public.fk_unique_parent USING btree (unique_code);

-- FK referencing the unique-index-backed column, not a PK.
ALTER TABLE ONLY public.fk_unique_child
    ADD CONSTRAINT fk_unique_child_parent_fkey FOREIGN KEY (parent_code) REFERENCES public.fk_unique_parent(unique_code);
