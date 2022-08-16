--
-- Name: fruits; Type: TABLE; Schema: public; Owner: owner
--

CREATE TABLE public.fruits (
    id integer,
    name character varying(20)
);


ALTER TABLE public.fruits OWNER TO owner;

--
-- Name: users; Type: TABLE; Schema: public; Owner: owner
--

CREATE TABLE public.users (
    id bigint NOT NULL,
    hash_firstname text NOT NULL,
    hash_lastname text NOT NULL,
    gender character varying(6) NOT NULL,
    CONSTRAINT users_gender_check CHECK (((gender)::text = ANY (ARRAY[('male'::character varying)::text, ('female'::character varying)::text])))
);


ALTER TABLE public.users OWNER TO owner;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: owner
--

CREATE SEQUENCE public.users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.users_id_seq OWNER TO owner;

--
-- Data for Name: fruits; Type: TABLE DATA; Schema: public; Owner: owner
--
INSERT INTO public.fruits (id, name)
    VALUES (1, 'watermelon'),
    (2, 'pear'),
    (3, 'strawberry'),
    (4, 'grape');

--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: owner
--

INSERT INTO public.users (id, hash_firstname, hash_lastname, gender)
    VALUES (1, 'garbagefirst', 'garbagelast', 'male'),
    (2, 'garbagefirst1', 'garbagelast1', 'female'),
    (3, 'sdgarbagefirst', 'dgsadsrbagelast', 'male'),
    (4, 'dsdssdgarbagefirst', 'dgsaggggdjjjsrbagelast', 'female'),
    (5, 'dsdssdgarbagefirt', 'dgsagggdjjjsrbagelast', 'female');


--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: owner
--

SELECT pg_catalog.setval('public.users_id_seq', 1, false);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: owner
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);
