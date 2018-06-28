--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.9
-- Dumped by pg_dump version 9.6.9

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: second; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.second (
    i integer NOT NULL,
    s text
);


ALTER TABLE public.second OWNER TO postgres;

--
-- Data for Name: second; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.second (i, s) FROM stdin;
0	0
1	1
2	2
3	3
4	4
5	5
6	6
\.


--
-- Name: second second_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.second
    ADD CONSTRAINT second_pkey PRIMARY KEY (i);


--
-- PostgreSQL database dump complete
--

