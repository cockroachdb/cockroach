--
-- PostgreSQL database dump
--

-- Dumped from database version 13.1
-- Dumped by pg_dump version 13.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: bar; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA bar;


ALTER SCHEMA bar OWNER TO postgres;

--
-- Name: baz; Type: SCHEMA; Schema: -; Owner: adityamaru
--

CREATE SCHEMA baz;


ALTER SCHEMA baz OWNER TO adityamaru;

--
-- Name: foo; Type: SCHEMA; Schema: -; Owner: adityamaru
--

CREATE SCHEMA foo;


ALTER SCHEMA foo OWNER TO adityamaru;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: test; Type: TABLE; Schema: bar; Owner: adityamaru
--

CREATE TABLE bar.test (
    id integer UNIQUE,
    name character varying
);

--
-- Name: test2; Type: TABLE; Schema: bar; Owner: adityamaru
--

CREATE TABLE bar.test2 (
    id integer,
    name character varying
);

--
-- Name: test2 testfk; Type: FK CONSTRAINT; Schema: bar; Owner: adityamaru
--

ALTER TABLE ONLY bar.test2 ADD CONSTRAINT testfk FOREIGN KEY (id) REFERENCES bar.test(id) ON DELETE CASCADE;

--
-- Name: testseq; Type: SEQUENCE; Schema: bar; Owner: adityamaru
--

CREATE SEQUENCE bar.testseq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE;


ALTER TABLE bar.test OWNER TO adityamaru;

--
-- Name: test; Type: TABLE; Schema: baz; Owner: adityamaru
--

CREATE TABLE baz.test (
    id integer,
    name character varying
);


ALTER TABLE baz.test OWNER TO adityamaru;

--
-- Name: test; Type: TABLE; Schema: foo; Owner: adityamaru
--

CREATE TABLE foo.test (
    id integer,
    name character varying
);


ALTER TABLE foo.test OWNER TO adityamaru;

--
-- Name: test; Type: TABLE; Schema: public; Owner: adityamaru
--

CREATE TABLE public.test (
    id integer,
    name character varying
);


ALTER TABLE public.test OWNER TO adityamaru;

--
-- Data for Name: test; Type: TABLE DATA; Schema: bar; Owner: adityamaru
--

COPY bar.test (id, name) FROM stdin;
1	abc
2	def
\.


--
-- Data for Name: test; Type: TABLE DATA; Schema: baz; Owner: adityamaru
--

COPY baz.test (id, name) FROM stdin;
1	abc
2	def
\.


--
-- Data for Name: test; Type: TABLE DATA; Schema: foo; Owner: adityamaru
--

COPY foo.test (id, name) FROM stdin;
1	abc
2	def
\.


--
-- Data for Name: test; Type: TABLE DATA; Schema: public; Owner: adityamaru
--

COPY public.test (id, name) FROM stdin;
1	abc
2	def
\.


--
-- PostgreSQL database dump complete
--

