--
-- PostgreSQL database dump
--

\restrict yXEWnaaKRt6ZjgHlzKexpygVaO4gBwZggzVm0VyN0yopEghWB91iS98HlNaD1cp

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
-- Data for Name: t; Type: TABLE DATA; Schema: esbdb; Owner: andreylartsev
--

COPY esbdb.t (eventid) FROM stdin;
1
\.


--
-- Data for Name: t1; Type: TABLE DATA; Schema: esbdb; Owner: andreylartsev
--

COPY esbdb.t1 (v1) FROM stdin;
1
2
\.


--
-- Data for Name: t2; Type: TABLE DATA; Schema: esbdb; Owner: andreylartsev
--

COPY esbdb.t2 (kk, created_at) FROM stdin;
1	2026-04-09 09:09:11.047579+03
2	2026-04-09 09:09:11.047579+03
\.


--
-- Name: t1_v1_seq; Type: SEQUENCE SET; Schema: esbdb; Owner: andreylartsev
--

SELECT pg_catalog.setval('esbdb.t1_v1_seq', 1, false);


--
-- PostgreSQL database dump complete
--

\unrestrict yXEWnaaKRt6ZjgHlzKexpygVaO4gBwZggzVm0VyN0yopEghWB91iS98HlNaD1cp

