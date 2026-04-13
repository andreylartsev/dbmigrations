--
-- PostgreSQL database dump
--

\restrict dvk33K18FehbG3yeXscbG4Qf4rFuIE8jgwggwDo7UPF0MF8TctaDFCeHfyvpyJu

-- Dumped from database version 18.3 (Homebrew)
-- Dumped by pg_dump version 18.3 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: esbdb; Type: SCHEMA; Schema: -; Owner: -
--

-- CREATE SCHEMA esbdb;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: t1; Type: TABLE; Schema: esbdb; Owner: -
--

CREATE TABLE esbdb.t1 (
    v1 integer NOT NULL
);


--
-- Name: latest_t1; Type: VIEW; Schema: esbdb; Owner: -
--

CREATE VIEW esbdb.latest_t1 AS
 SELECT max(v1) AS v1
   FROM esbdb.t1;


--
-- Name: t; Type: TABLE; Schema: esbdb; Owner: -
--

CREATE TABLE esbdb.t (
    eventid character varying(36) NOT NULL
);


--
-- Name: t1_v1_seq; Type: SEQUENCE; Schema: esbdb; Owner: -
--

CREATE SEQUENCE esbdb.t1_v1_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: t1_v1_seq; Type: SEQUENCE OWNED BY; Schema: esbdb; Owner: -
--

ALTER SEQUENCE esbdb.t1_v1_seq OWNED BY esbdb.t1.v1;


--
-- Name: t2; Type: TABLE; Schema: esbdb; Owner: -
--

CREATE TABLE esbdb.t2 (
    kk character varying(36) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: t1 v1; Type: DEFAULT; Schema: esbdb; Owner: -
--

ALTER TABLE ONLY esbdb.t1 ALTER COLUMN v1 SET DEFAULT nextval('esbdb.t1_v1_seq'::regclass);


--
-- Name: t1 t1_pkey; Type: CONSTRAINT; Schema: esbdb; Owner: -
--

ALTER TABLE ONLY esbdb.t1
    ADD CONSTRAINT t1_pkey PRIMARY KEY (v1);


--
-- Name: t2 t2_pkey; Type: CONSTRAINT; Schema: esbdb; Owner: -
--

ALTER TABLE ONLY esbdb.t2
    ADD CONSTRAINT t2_pkey PRIMARY KEY (kk);


--
-- Name: t t_pkey; Type: CONSTRAINT; Schema: esbdb; Owner: -
--

ALTER TABLE ONLY esbdb.t
    ADD CONSTRAINT t_pkey PRIMARY KEY (eventid);


--
-- PostgreSQL database dump complete
--

\unrestrict dvk33K18FehbG3yeXscbG4Qf4rFuIE8jgwggwDo7UPF0MF8TctaDFCeHfyvpyJu

