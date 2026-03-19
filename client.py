"""
    Trabalho 1 - Sistemas Distribuídos - 2024/1
    Grupo 6
    Estudantes: Leo Rodrigues Aoki - 801926, João Eduardo Batelochi Altarugio - 800815,  Gabriel Costa de Lucca - 800212
"""

import zmq
import threading
import numpy as np
import cv2 as cv
import tkinter as tk
import base64
import zlib
from tkinter import scrolledtext
import pyaudio
import time

broker_ip = "192.168.87.99"

class User:
    def __init__(self, pub_id, topic):
        self.pub_id = pub_id
        self.topic = topic
        self.context = zmq.Context()

        print("Iniciando conexões...")
        self.conectPubText()
        self.conectSubText()
        self.conectPubVideo()
        self.conectSubVideo()
        self.conectPubAudio()
        self.conectSubAudio()

        print("Iniciando threads de I/O...")
        self.threads = []
        self.execIOText()
        self.execIVideo()
        self.execIAudio()

        print("Inicializando UI...")
        self.initUI()

    def initUI(self):
        self.root = tk.Tk()
        self.root.title("Meet")

        self.chat_display = scrolledtext.ScrolledText(self.root, wrap=tk.WORD)
        self.chat_display.grid(row=0, column=0, columnspan=2)

        self.msg_entry = tk.Entry(self.root, width=50)
        self.msg_entry.grid(row=1, column=0)
        self.msg_entry.bind("<Return>", self.sendMessages)

        self.send_button = tk.Button(self.root, text="Send", command=self.sendMessages)
        self.send_button.grid(row=1, column=1)

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def on_closing(self):
        print("Fechando aplicação...")
        self.root.quit()
        self.context.term()

        for thread in self.threads:
            thread.join()

    def execIOText(self):
        self.receive_thread = threading.Thread(target=self.receiveMessages, daemon=True)
        self.receive_thread.start()
        self.threads.append(self.receive_thread)

    def execIVideo(self):
        self.send_thread_video = threading.Thread(target=self.sendVideo, daemon=True)
        self.send_thread_video.start()
        self.threads.append(self.send_thread_video)

        self.receive_thread_video = threading.Thread(target=self.receiveVideo, daemon=True)
        self.receive_thread_video.start()
        self.threads.append(self.receive_thread_video)

    def execIAudio(self):
        self.send_thread_audio = threading.Thread(target=self.sendAudio, daemon=True)
        self.send_thread_audio.start()
        self.threads.append(self.send_thread_audio)

        self.receive_thread_audio = threading.Thread(target=self.receiveAudio, daemon=True)
        self.receive_thread_audio.start()
        self.threads.append(self.receive_thread_audio)

    def sendMessages(self, event=None):
        message = self.msg_entry.get()
        self.pub_socket.send_string(f"{self.topic};{message};{self.pub_id}")
        self.msg_entry.delete(0, tk.END)

    def receiveMessages(self):
        while True:
            data_bytes = self.sub_socket.recv()
            parts = data_bytes.split(b';')
            if len(parts) == 3:
                topic, message, publisher_id = parts
                self.chat_display.insert(tk.END, f"({publisher_id.decode()}) : {message.decode()}\n")
                self.chat_display.yview(tk.END)

    def conectPubText(self):
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect(f"tcp://{broker_ip}:5555")

    def conectSubText(self):
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(f"tcp://{broker_ip}:5556")
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)

    def conectPubVideo(self):
        self.pub_socket_video = self.context.socket(zmq.PUB)
        self.pub_socket_video.connect(f"tcp://{broker_ip}:5589")

    def conectSubVideo(self):
        self.sub_socket_video = self.context.socket(zmq.SUB)
        self.sub_socket_video.connect(f"tcp://{broker_ip}:5590")
        self.sub_socket_video.setsockopt_string(zmq.SUBSCRIBE, self.topic)

    def conectPubAudio(self):
        self.pub_socket_audio = self.context.socket(zmq.PUB)
        self.pub_socket_audio.connect(f"tcp://{broker_ip}:6000")

    def conectSubAudio(self):
        self.sub_socket_audio = self.context.socket(zmq.SUB)
        self.sub_socket_audio.connect(f"tcp://{broker_ip}:6001")
        self.sub_socket_audio.setsockopt_string(zmq.SUBSCRIBE, self.topic)

    def sendVideo(self):
        cap = cv.VideoCapture(0)

        if not cap.isOpened():
            print("Não foi possível abrir a câmera.")
            return

        # Definindo uma resolução menor para reduzir a quantidade de dados
        cap.set(cv.CAP_PROP_FRAME_WIDTH, 320)
        cap.set(cv.CAP_PROP_FRAME_HEIGHT, 240)
        # Reduzindo a taxa de quadros para diminuir a quantidade de dados
        cap.set(cv.CAP_PROP_FPS, 15)

        try:
            while True:
                ret, frame = cap.read() 
                if not ret:
                    print("Erro ao ler o frame da câmera.")
                    break

                cv.imshow("Camera local", frame)

                # Codificando e comprimindo o frame
                _, buffer = cv.imencode('.jpg', frame, [int(cv.IMWRITE_JPEG_QUALITY), 50])  # Ajuste a qualidade da imagem para reduzir o tamanho

                # Compressão zlib
                compressed_frame = zlib.compress(buffer, level=1)  # Usando compressão rápida

                # Enviando o tópico, o publicador e os dados do frame comprimidos diretamente
                self.pub_socket_video.send_multipart([self.topic.encode(), self.pub_id.encode(), compressed_frame])

                if cv.waitKey(1) & 0xFF == ord('q'):
                    break
        finally:
            cap.release()
            cv.destroyAllWindows()

    def receiveVideo(self):
        try:
            while True:
                
                topic, pub_ID, compressed_frame = self.sub_socket_video.recv_multipart()
                
                if pub_ID.decode() != self.pub_id:
                    if not compressed_frame:
                        print("Nenhum dado recebido.")
                        continue

                    # Decompressão dos dados do frame
                    try:
                        decompressed_frame = zlib.decompress(compressed_frame)
                    except Exception as e:
                        print(f"Erro ao descomprimir dados: {e}")
                        continue

                    # Convertendo de volta para o frame
                    try:
                        jpg_as_np = np.frombuffer(decompressed_frame, dtype=np.uint8)
                        frame = cv.imdecode(jpg_as_np, flags=cv.IMREAD_COLOR)
                    except Exception as e:
                        print(f"Erro ao decodificar frame: {e}")
                        continue

                    # Exibindo o frame apenas se o ID do publisher for diferente
                    cv.namedWindow('Participante', cv.WINDOW_NORMAL)
                    if frame is not None:
                        cv.imshow("Participante", frame)
                    else:
                        print("Frame recebido inválido.")

                    if cv.waitKey(1) & 0xFF == ord('q'):
                        break
        finally:
            cv.destroyAllWindows()

    def sendAudio(self):
        p = pyaudio.PyAudio()
        stream = p.open(format=pyaudio.paInt16,
                        channels=1,
                        rate=44100,
                        input=True,
                        frames_per_buffer=1024)

        try:
            while True:
                try:
                    data = stream.read(1024, exception_on_overflow=False)
                    print("SEND read\n")
                    base64_audio = base64.b64encode(data).decode('utf-8')
                    print("SEND Decoded\n")
                    self.pub_socket_audio.send_multipart([self.topic.encode(), self.pub_id.encode(), base64_audio.encode()])
                    print("SEND Áudio enviado com sucesso.")
                except Exception as e:
                    print(f"Erro ao enviar áudio: {e}")

        finally:
            stream.stop_stream()
            stream.close()
            p.terminate()

    def receiveAudio(self):
        print("Entrei na funcao receive\n")
        time.sleep(1)
        p = pyaudio.PyAudio()
        stream = p.open(format=pyaudio.paInt16,
                        channels=1,
                        rate=44100,
                        output=True,
                        frames_per_buffer=1024)
        try:
            while True:
                try:
                    print("RECV To dentro do while\n")
                    topic, pub_ID, base64_audio = self.sub_socket_audio.recv_multipart(zmq.NOBLOCK)
                    print("RECV Passei do recv\n")

                    print("RECV TO DENTRO DECODIFICANDO")
                    data = base64.b64decode(base64_audio)
                    # print(f"printando base64 {data}")
                    
                    if pub_ID.decode() != self.pub_id and topic.decode() == self.topic:
                        stream.write(data)
                        print("Áudio recebido e reproduzido com sucesso.")
                except Exception as e:
                    print(f"Erro ao decodificar ou reproduzir áudio: {e}")

        finally:
            stream.stop_stream()
            stream.close()
            p.terminate()

def main():
    pub_id = input("Qual seu nome? ")
    topic = input("Digite o tópico por favor: ")
    user = User(pub_id, topic)
    user.root.mainloop()

if __name__ == "__main__":
    main()
