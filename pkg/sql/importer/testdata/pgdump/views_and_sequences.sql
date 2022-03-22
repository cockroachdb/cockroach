--
-- PostgreSQL database dump
--

-- Dumped from database version 13.2
-- Dumped by pg_dump version 13.2

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
-- Name: s; Type: SEQUENCE; Schema: public; Owner: otan
--

CREATE SEQUENCE public.s
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.s OWNER TO otan;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: tbl; Type: TABLE; Schema: public; Owner: otan
--

CREATE TABLE public.tbl (
    a integer,
    b integer
);


ALTER TABLE public.tbl OWNER TO otan;

--
-- Name: v; Type: VIEW; Schema: public; Owner: otan
--

CREATE VIEW public.v AS
 SELECT tbl.a
   FROM public.tbl;


ALTER TABLE public.v OWNER TO otan;

--
-- Data for Name: tbl; Type: TABLE DATA; Schema: public; Owner: otan
--

COPY public.tbl (a, b) FROM stdin;
\.


--
-- Name: s; Type: SEQUENCE SET; Schema: public; Owner: otan
--

SELECT pg_catalog.setval('public.s', 1, false);


--
-- PostgreSQL database dump complete
--

