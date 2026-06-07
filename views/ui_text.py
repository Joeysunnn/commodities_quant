"""Small bilingual text helpers for Streamlit views."""

import html

import streamlit as st


def bilingual_page_title(english: str, chinese: str, icon: str = "") -> None:
    icon_html = f"{icon} " if icon else ""
    st.markdown(
        f"""
        <div class="bilingual-title" style="text-align:center; padding:0.75rem 0 0.25rem 0;">
            <h1 style="color:#1f77b4; font-size:2.75rem; line-height:1.15; margin:0; font-weight:800;">
                {icon_html}{html.escape(english)}
            </h1>
            <p style="color:#6b7280; font-size:1rem; margin:0.35rem 0 0 0;">
                {html.escape(chinese)}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def bilingual_section(english: str, chinese: str, icon: str = "") -> None:
    icon_html = f"{icon} " if icon else ""
    st.markdown(
        f"""
        <div class="bilingual-section" style="margin:0.35rem 0 0.8rem 0;">
            <h3 style="color:#262730; font-size:1.65rem; line-height:1.2; margin:0; font-weight:750;">
                {icon_html}{html.escape(english)}
            </h3>
            <p style="color:#6b7280; font-size:0.9rem; margin:0.2rem 0 0 0;">
                {html.escape(chinese)}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def bilingual_chart_title(index: str, english: str, chinese: str, icon: str = "") -> None:
    icon_html = f"{icon} " if icon else ""
    prefix = f"{html.escape(index)}. " if index else ""
    st.markdown(
        f"""
        <div class="bilingual-chart-title" style="margin:0.25rem 0 0.6rem 0;">
            <h4 style="color:#262730; font-size:1.05rem; line-height:1.25; margin:0; font-weight:700;">
                {prefix}{icon_html}{html.escape(english)}
            </h4>
            <p style="color:#6b7280; font-size:0.9rem; margin:0.2rem 0 0 0;">
                {html.escape(chinese)}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def bilingual_sidebar_title(english: str, chinese: str, icon: str = "") -> None:
    icon_html = f"{icon} " if icon else ""
    st.sidebar.markdown(
        f"""
        <div class="bilingual-sidebar-title">
            <h2 style="color:#262730; font-size:1.55rem; line-height:1.2; margin:0.2rem 0 0 0; font-weight:800;">
                {icon_html}{html.escape(english)}
            </h2>
            <p style="color:#6b7280; font-size:0.9rem; margin:0.2rem 0 0 0;">
                {html.escape(chinese)}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def bilingual_sidebar_section(english: str, chinese: str, icon: str = "") -> None:
    icon_html = f"{icon} " if icon else ""
    st.sidebar.markdown(
        f"""
        <div class="bilingual-sidebar-section" style="margin:0.25rem 0 0.75rem 0;">
            <h3 style="color:#262730; font-size:1.15rem; line-height:1.2; margin:0; font-weight:750;">
                {icon_html}{html.escape(english)}
            </h3>
            <p style="color:#6b7280; font-size:0.82rem; margin:0.15rem 0 0 0;">
                {html.escape(chinese)}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
