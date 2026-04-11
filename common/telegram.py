from __future__ import annotations

import streamlit as st
import requests


def _send_telegram(token: str, chat_id: str, text: str) -> dict:
    """텔레그램 Bot API로 메시지 전송. 결과 dict 반환."""
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        return resp.json()
    except Exception as e:
        return {"ok": False, "description": str(e)}


def render_telegram_help_popover(strategy_name: str = "표준편차",
                                 example_bot_display: str = "표준편차 알림봇",
                                 example_bot_username: str = "stdev_alert_bot",
                                 example_bot_username2: str = "my_soxl_bot",
                                 test_button_label: str = "📨 주문표 테스트 발송"):
    """텔레그램 Bot Token / Chat ID 설정 가이드를 st.markdown HTML로 렌더링한다.
    반드시 ``with st.popover(...)`` 블록 안에서 호출해야 한다.

    Parameters
    ----------
    strategy_name : str
        전략 표시 이름 (예: "표준편차", "Sigma", "3일평균").
    example_bot_display : str
        봇 표시 이름 예시 (예: "표준편차 알림봇").
    example_bot_username : str
        봇 username 예시 (예: "stdev_alert_bot").
    example_bot_username2 : str
        봇 username 두 번째 예시 (예: "my_soxl_bot").
    test_button_label : str
        4단계 연결 테스트에서 언급할 버튼 라벨.
    """
    st.markdown(f"""
    <style>
    .tg-help-section {{ margin-bottom: 20px; }}
    .tg-help-title {{
        display: flex; align-items: center; gap: 10px;
        font-size: 17px; font-weight: 700; color: #1a1a2e; margin-bottom: 10px;
    }}
    .tg-help-badge {{
        background: #4A90D9; color: white;
        border-radius: 50%; width: 28px; height: 28px;
        display: inline-flex; align-items: center; justify-content: center;
        font-size: 14px; font-weight: 700; flex-shrink: 0;
    }}
    .tg-help-box {{
        background: #EEF4FB; border-radius: 10px;
        padding: 14px 18px; font-size: 14px; line-height: 2;
    }}
    .tg-help-box ol {{ margin: 0; padding-left: 20px; }}
    .tg-help-box li {{ margin-bottom: 2px; }}
    .tg-tag {{
        background: #D0E8FF; color: #1a5fa8;
        border-radius: 5px; padding: 1px 7px;
        font-family: monospace; font-size: 13px;
    }}
    .tg-code-box {{
        background: #1e2533; color: #7dd3fc;
        border-radius: 8px; padding: 10px 14px; margin-top: 8px;
        font-family: monospace; font-size: 12px; word-break: break-all;
        line-height: 1.7;
    }}
    .tg-example-box {{
        background: white; border: 1px solid #CBD5E1; border-radius: 8px;
        padding: 12px 16px; margin-top: 10px; font-size: 13px; color: #555;
    }}
    .tg-example-val {{ color: #4A90D9; font-family: monospace; font-size: 13px; }}
    .tg-warn-box {{
        background: #FFFBEB; border: 1px solid #F59E0B;
        border-radius: 10px; padding: 14px 18px; font-size: 14px; line-height: 2;
    }}
    .tg-warn-title {{ font-weight: 700; color: #92400E; margin-bottom: 4px; }}
    .tg-sub-title {{ font-weight: 700; color: #1a5fa8; margin: 10px 0 4px 0; }}
    .tg-tip-box {{
        background: #F0FDF4; border: 1px solid #86EFAC;
        border-radius: 8px; padding: 10px 14px; margin-top: 8px;
        font-size: 13px; color: #166534;
    }}
    </style>

    <div class="tg-help-section">
      <div class="tg-help-title"><span class="tg-help-badge">1</span> Bot Token 생성하기</div>
      <div class="tg-help-box">
        <ol>
          <li>텔레그램 앱에서 검색창에 <span class="tg-tag">@BotFather</span> 를 검색합니다.</li>
          <li>파란 체크 공식 계정을 선택하고 <span class="tg-tag">/start</span> 를 눌러 대화를 시작합니다.</li>
          <li><span class="tg-tag">/newbot</span> 을 입력합니다.</li>
          <li><strong>봇 표시 이름</strong>을 입력합니다. (예: <span class="tg-tag">{example_bot_display}</span>) — 한글 가능, 자유롭게 설정</li>
          <li><strong>봇 username</strong>을 입력합니다. — 영문+숫자만 가능, 반드시 <span class="tg-tag">bot</span> 으로 끝나야 함<br>
              &nbsp;&nbsp;예: <span class="tg-tag">{example_bot_username}</span> &nbsp;/&nbsp; <span class="tg-tag">{example_bot_username2}</span></li>
          <li>성공 시 <strong>HTTP API Token</strong>이 발급됩니다. 이것이 <strong>Bot Token</strong>입니다.</li>
        </ol>
        <div class="tg-example-box">
          <div style="color:#888; font-size:12px; margin-bottom:4px;">Bot Token 예시 (발급 후 복사해서 아래 입력창에 붙여넣기):</div>
          <div class="tg-example-val">1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ</div>
        </div>
      </div>
    </div>

    <div class="tg-help-section">
      <div class="tg-help-title"><span class="tg-help-badge">2</span> 내 봇 시작하기 (필수!)</div>
      <div class="tg-warn-box">
        <div class="tg-warn-title">⚠ 봇을 먼저 시작해야 Chat ID를 확인하고 메시지를 받을 수 있습니다!</div>
        <ol>
          <li>텔레그램 검색창에서 내가 만든 봇 username을 검색합니다. (예: <span class="tg-tag">@{example_bot_username}</span>)</li>
          <li>봇 대화창에서 <span class="tg-tag">/start</span> 를 눌러 봇을 활성화합니다.</li>
          <li>봇에게 아무 메시지나 한 번 보냅니다. (Chat ID 확인을 위해 필요)</li>
        </ol>
      </div>
    </div>

    <div class="tg-help-section">
      <div class="tg-help-title"><span class="tg-help-badge">3</span> Chat ID 확인하기</div>
      <div class="tg-help-box">
        <div class="tg-sub-title">✅ 방법 1: getUpdates API 사용 (가장 확실한 방법)</div>
        <ol>
          <li>위 2단계에서 봇에게 메시지를 보낸 후, 아래 주소를 브라우저에 입력합니다.</li>
          <li><span class="tg-tag">{{토큰값}}</span> 부분을 발급받은 Bot Token으로 교체합니다.</li>
        </ol>
        <div class="tg-code-box">https://api.telegram.org/bot<span style="color:#fde047;">{{토큰값}}</span>/getUpdates</div>
        <ol start="3">
          <li>JSON 응답에서 <span class="tg-tag">"id"</span> 값을 찾습니다. 이것이 <strong>Chat ID</strong>입니다.</li>
        </ol>
        <div class="tg-example-box">
          <div style="color:#888; font-size:12px; margin-bottom:6px;">응답 예시:</div>
          <div style="font-family:monospace; font-size:12px; color:#333; line-height:1.8;">
            {{"ok":true,"result":[{{"message":{{"chat":{{"id": 123456789}},"first_name":"홍길동"}}}}]}}
          </div>
        </div>
        <div class="tg-sub-title">방법 2: @userinfobot 사용 (간편)</div>
        <ol>
          <li>텔레그램에서 <span class="tg-tag">@userinfobot</span> 을 검색합니다.</li>
          <li><span class="tg-tag">/start</span> 를 누르면 자동으로 내 Chat ID가 표시됩니다.</li>
        </ol>
        <div class="tg-sub-title">방법 3: @RawDataBot 사용</div>
        <ol>
          <li>텔레그램에서 <span class="tg-tag">@RawDataBot</span> 을 검색합니다.</li>
          <li>아무 메시지나 보내면 JSON 형식으로 정보가 표시되며, <span class="tg-tag">"id"</span> 값이 Chat ID입니다.</li>
        </ol>
        <div class="tg-example-box">
          <div style="color:#888; font-size:12px; margin-bottom:4px;">Chat ID 예시 (숫자만, 복사해서 아래 입력창에 붙여넣기):</div>
          <div class="tg-example-val">123456789</div>
        </div>
      </div>
    </div>

    <div class="tg-help-section">
      <div class="tg-help-title"><span class="tg-help-badge">4</span> 연결 테스트</div>
      <div class="tg-tip-box">
        💡 Bot Token과 Chat ID를 입력한 후 아래 <strong>{test_button_label}</strong> 버튼을 눌러보세요.<br>
        메시지가 정상적으로 수신되면 설정 완료입니다! ✅
      </div>
    </div>
    """, unsafe_allow_html=True)
