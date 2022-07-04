# %%[markdown]
# font 混淆 识别
# 1. [Python如何解析TTF](https://www.modb.pro/db/175186)
# 2. [利用Python理解TTF矢量字体显示原理](https://blog.csdn.net/CQZHOUZR/article/details/116484309)
# 3. [百度字体编辑器](https://kekee000.github.io/fonteditor/index-en.html)
# 4. [OpenCV Freetype](https://www.pythonheidong.com/blog/article/327766/a72be84affd143fcc7f1/)
# 5. [Python/Matplotlib - 更改子图的相对大小](https://qa.1r1g.com/sf/ask/355863441/)
# %%
import re
import os
import pathlib
import pickle
import tempfile
from io import BytesIO
from functools import reduce
from PIL import Image
import numpy as np
from tqdm.auto import tqdm

from fontTools.ttLib import TTFont
from fontTools.pens.freetypePen import FreeTypePen
from fontTools.misc.transform import Offset

from fontTools.pens.svgPathPen import SVGPathPen
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.path import Path
import matplotlib.gridspec as gridspec

from baidu_orc import Baidu_ORC


# %%
class Autohome_Font_Matplotlib:
    def __init__(self) -> None:
        self.baidu_orc = Baidu_ORC()
        self.diranme = pathlib.Path('font')
        if not self.diranme.exists():
            self.diranme.mkdir()

    def read_font_from_file(self, filename_ttf):
        font = TTFont(filename_ttf)
        self.get_font_info(font)

    def get_font_info(self, font):
        self.font = font
        self.xMin = self.font['head'].xMin
        self.yMin = self.font['head'].yMin
        self.xMax = self.font['head'].xMax
        self.yMax = self.font['head'].yMax
        glyphorder_table = self.font.getGlyphOrder()
        self.uni_names = glyphorder_table[1:]

    def get_commands_by_uni_name(self, uni_name):
        glyphset = self.font.getGlyphSet()
        # 获取pen的基类
        pen = SVGPathPen(glyphset)
        glyph = glyphset[uni_name]
        glyph.draw(pen)
        commands = pen._commands
        return commands

    def get_total_commands(self, commands):
        total_commands = []
        command = []
        for i in commands:
            # 每一个命令语句
            if i == 'Z':
                # 以闭合路径指令Z区分不同轮廓线
                command.append(i)
                total_commands.append(command)
                command = []
            else:
                command.append(i)
        return total_commands

    def get_verts_codes(self, total_commands):
        # 笔的当前位置
        preX = 0.0
        preY = 0.0
        # 笔的起始位置
        startX = 0.0
        startY = 0.0
        # 所有轮廓点
        total_verts = []
        # 所有指令
        total_codes = []
        # 转换命令
        for i in total_commands:
            # 每一条轮廓线
            verts = []
            codes = []
            for command in i:
                # 每一条轮廓线中的每一个命令
                code = command[0]  # 第一个字符是指令
                vert = command[1:].split(' ')  # 其余字符是坐标点，以空格分隔
                # M = 路径起始 - 参数 - 起始点坐标 (x y)+
                if code == 'M':
                    codes.append(Path.MOVETO)  # 转换指令
                    verts.append((float(vert[0]), float(vert[1])))  # 提取x和y坐标
                    # 保存笔的起始位置
                    startX = float(vert[0])
                    startY = float(vert[1])
                    # 保存笔的当前位置(由于是起笔，所以当前位置就是起始位置)
                    preX = float(vert[0])
                    preY = float(vert[1])
                # Q = 绘制二次贝塞尔曲线 - 参数 - 曲线控制点和终点坐标(x1 y1 x y)+
                elif code == 'Q':
                    codes.append(Path.CURVE3)  #转换指令
                    verts.append((float(vert[0]), float(vert[1])))  #提取曲线控制点坐标
                    codes.append(Path.CURVE3)  #转换指令
                    verts.append((float(vert[2]), float(vert[3])))  #提取曲线终点坐标
                    # 保存笔的当前位置--曲线终点坐标x和y
                    preX = float(vert[2])
                    preY = float(vert[3])
                # C = 绘制三次贝塞尔曲线 - 参数 - 曲线控制点1，控制点2和终点坐标(x1 y1 x2 y2 x y)+
                elif code == 'C':
                    codes.append(Path.CURVE4)  #转换指令
                    verts.append((float(vert[0]), float(vert[1])))  #提取曲线控制点1坐标
                    codes.append(Path.CURVE4)  #转换指令
                    verts.append((float(vert[2]), float(vert[3])))  #提取曲线控制点2坐标
                    codes.append(Path.CURVE4)  #转换指令
                    verts.append((float(vert[4]), float(vert[5])))  #提取曲线终点坐标
                    # 保存笔的当前位置--曲线终点坐标x和y
                    preX = float(vert[4])
                    preY = float(vert[5])
                # L = 绘制直线 - 参数 - 直线终点(x, y)+
                elif code == 'L':
                    codes.append(Path.LINETO)  #转换指令
                    verts.append((float(vert[0]), float(vert[1])))  #提取直线终点坐标
                    # 保存笔的当前位置--直线终点坐标x和y
                    preX = float(vert[0])
                    preY = float(vert[1])
                # V = 绘制垂直线 - 参数 - 直线y坐标 (y)+
                elif code == 'V':
                    # 由于是垂直线，x坐标不变，提取y坐标
                    x = preX
                    y = float(vert[0])
                    codes.append(Path.LINETO)  #转换指令
                    verts.append((x, y))  #提取直线终点坐标
                    # 保存笔的当前位置--直线终点坐标x和y
                    preX = x
                    preY = y
                # H = 绘制水平线 - 参数 - 直线x坐标 (x)+
                elif code == 'H':
                    # 由于是水平线，y坐标不变，提取x坐标
                    x = float(vert[0])
                    y = preY
                    codes.append(Path.LINETO)  #转换指令
                    verts.append((x, y))  #提取直线终点坐标
                    # 保存笔的当前位置--直线终点坐标x和y
                    preX = x
                    preY = y
                # Z = 路径结束，无参数
                elif code == 'Z':
                    codes.append(Path.CLOSEPOLY)  #转换指令
                    verts.append((startX, startY))  #终点坐标就是路径起点坐标
                    #保存笔的当前位置--起点坐标x和y
                    preX = startX
                    preY = startY
                # 有一些语句指令为空，当作直线处理
                else:
                    codes.append(Path.LINETO)  #转换指令
                    verts.append((float(vert[0]), float(vert[1])))  #提取直线终点坐标
                    # 保存笔的当前位置--直线终点坐标x和y
                    preX = float(vert[0])
                    preY = float(vert[1])
            # 整合所有指令和坐标
            total_verts.append(verts)
            total_codes.append(codes)
        return total_verts, total_codes

    def get_contour_points(self, total_verts):
        points_x = []
        points_y = []
        for contour in total_verts:
            # 每一条轮廓曲线
            x = []
            y = []
            for i in contour:
                # 轮廓线上每一个点的坐标(x,y)
                x.append(i[0])
                y.append(i[1])
            points_x.append(x)
            points_y.append(y)
        return points_x, points_y

    def count_path_contains(self, paths_dict, i):
        self = paths_dict[i]
        count = 0
        for k, v in paths_dict.items():
            if k != i and v.contains_points(self._vertices).all():
                count += 1
        return count

    def get_path_plot_dict(self, total_verts, total_codes):
        path_dict = {
            i: Path(total_verts[i], total_codes[i])
            for i in range(len(total_verts))
        }
        path_plot_dict = {}
        for i in path_dict:
            res = self.count_path_contains(path_dict, i)
            if res in path_plot_dict:
                path_plot_dict[res].append(path_dict[i])
            else:
                path_plot_dict[res] = [path_dict[i]]
        path_plot_dict = {
            k: path_plot_dict[k]
            for k in sorted(list(path_plot_dict.keys()))
        }
        return path_plot_dict

    def plot_single_font(self, path_plot_dict, mode='bw'):
        # 创建画布窗口
        fig, ax = plt.subplots()
        # 按照'head'表中所有字形的边界框设定x和y轴上下限
        ax.set_xlim(self.xMin, self.xMax)
        ax.set_ylim(self.yMin, self.yMax)
        # 不显示坐标
        ax.set_xticks([])
        ax.set_yticks([])
        # 设置画布1:1显示
        ax.set_aspect(1)
        # 添加网格线
        # ax.grid(alpha=0.8,linestyle='--')
        # 画图
        for i in path_plot_dict:
            if mode == 'bw':
                if i % 2:
                    for path in path_plot_dict[i]:
                        patch = patches.PathPatch(path,
                                                  facecolor='white',
                                                  edgecolor='black',
                                                  lw=2)
                        ax.add_patch(patch)
                else:
                    for path in path_plot_dict[i]:
                        patch = patches.PathPatch(path,
                                                  facecolor='black',
                                                  edgecolor='black',
                                                  lw=2)
                        ax.add_patch(patch)
            elif mode == 'transparent':
                for path in path_plot_dict[i]:
                    patch = patches.PathPatch(path,
                                              facecolor='none',
                                              edgecolor='black',
                                              lw=2)
                    ax.add_patch(patch)
        return plt

    def plot_by_uni_name(self, uni_name, mode='bw', show=False):
        commands = self.get_commands_by_uni_name(uni_name)
        total_commands = self.get_total_commands(commands)
        total_verts, total_codes = self.get_verts_codes(total_commands)
        # points_x, points_y = self.get_contour_points(total_verts) 轮廓点
        path_plot_dict = self.get_path_plot_dict(total_verts, total_codes)
        plt = self.plot_single_font(path_plot_dict, mode)
        # 保存图片
        filename = self.diranme.joinpath('%s.png' % uni_name)
        plt.savefig(filename)
        if show:
            plt.show()
        return filename

    def plot_all_in_row(self, path_plot_dict_list, mode='bw', show=False):
        lenth = len(path_plot_dict_list)
        fig = plt.figure(figsize=(4 * lenth, 4), facecolor='white')
        gs = gridspec.GridSpec(1, lenth, width_ratios=[1] * lenth)

        # plt.subplots(1,len(path_plot_dict_list[:10]))
        # path_plot_dict_list = path_plot_dict_list[:2]

        for idx, path_plot_dict in enumerate(path_plot_dict_list):
            # 按照'head'表中所有字形的边界框设定x和y轴上下限
            ax = plt.subplot(gs[idx])
            ax.set_xlim(self.xMin, self.xMax)
            ax.set_ylim(self.yMin, self.yMax)
            # 不显示坐标
            ax.set_xticks([])
            ax.set_yticks([])
            # 设置画布1:1显示
            ax.set_aspect(1)
            # 添加网格线
            # ax.grid(alpha=0.8,linestyle='--')
            for i in path_plot_dict:
                if mode == 'bw':
                    if i % 2:
                        for path in path_plot_dict[i]:
                            patch = patches.PathPatch(path,
                                                      facecolor='white',
                                                      edgecolor='black',
                                                      lw=2)
                            ax.add_patch(patch)
                    else:
                        for path in path_plot_dict[i]:
                            patch = patches.PathPatch(path,
                                                      facecolor='black',
                                                      edgecolor='black',
                                                      lw=2)
                            ax.add_patch(patch)
                elif mode == 'transparent':
                    for path in path_plot_dict[i]:
                        patch = patches.PathPatch(path,
                                                  facecolor='none',
                                                  edgecolor='black',
                                                  lw=2)
                        ax.add_patch(patch)

        # 保存图片
        filename = tempfile.TemporaryFile(delete=False, suffix='.png')
        plt.savefig(filename.name)
        if show:
            plt.show()
        return filename

    def generate_fonts_dict(self, fonts):
        # 识别文字
        with tqdm(total=len(fonts), desc='recognize texts from baidu') as pbar:
            fonts_dict = {}
            for k, font in fonts.items():
                pbar.update(1)
                self.get_font_info(font)
                path_plot_dict_list = []
                for uni_name in self.uni_names:
                    commands = self.get_commands_by_uni_name(uni_name)
                    total_commands = self.get_total_commands(commands)
                    total_verts, total_codes = self.get_verts_codes(
                        total_commands)
                    # points_x, points_y = self.get_contour_points(total_verts)
                    path_plot_dict = self.get_path_plot_dict(
                        total_verts, total_codes)
                    path_plot_dict_list.append(path_plot_dict)

                # 作图
                im_filename = self.plot_all_in_row(path_plot_dict_list, 'bw',
                                                   False)
                baidu_result = self.baidu_orc.general_basic(im_filename.name)
                im_filename.close()
                os.remove(im_filename.name)

                gnames = font.getGlyphNames()[1:]
                # 调整名称为之家文字中的格式, 示例\uede4
                gname_im_dict = {
                    eval(r"u'\u" + k[3:] + "'"): None
                    for k in gnames
                }
                if baidu_result is not None:
                    gname_from_baidu = ''.join(
                        [w['words'] for w in baidu_result['words_result']])
                    if len(gname_im_dict) == len(gname_from_baidu):
                        gname_im_dict_recognized = {
                            k: v
                            for k, v in zip(gname_im_dict, gname_from_baidu)
                        }
                    fonts_dict[k] = gname_im_dict_recognized
        return fonts_dict


class Autohome_Font_Freetypepen:
    def __init__(self) -> None:
        self.baidu_orc = Baidu_ORC()

    def freetypepen_plot(self, font, gname, show=False):
        pen = FreeTypePen(None)  # 实例化Pen子类
        glyph = font.getGlyphSet()[gname]  # 通过字形名称选择某一字形对象
        glyph.draw(pen)  # “画”出字形轮廓
        if show:
            width, ascender, descender = glyph.width, font[
                'OS/2'].usWinAscent, -font['OS/2'].usWinDescent
            # 获取字形的宽度和上沿以及下沿
            height = ascender - descender  # 利用上沿和下沿计算字形高度
            pen.show(width=width,
                     height=height,
                     transform=Offset(0, -descender))  # 显示以及矫正
        try:
            im = pen.image()
            return im
        except Exception as e:
            # print(e)
            return

    def get_font_gname_im_dict(self, font):
        # 如果画图错误, 则v=None
        gnames = font.getGlyphNames()[1:]
        gname_im_dict = {
            gname: self.freetypepen_plot(font, gname, show=False)
            for gname in gnames
        }

        # 调整名称为之家文字中的格式, 示例\uede4
        gname_im_dict = {
            eval(r"u'\u" + k[3:] + "'"): v
            for k, v in gname_im_dict.items()
        }
        return gname_im_dict

    def im_bw_transpose(self, im):
        # 黑白互换
        im_arr = np.array(im)
        im_arr = 255 - im_arr
        im_bw = Image.fromarray(im_arr)
        return im_bw

    def im_put_to_center(self, im, ratio=2):
        # 居中
        im_center = Image.new(im.mode, im.size, 255)
        ratio_size = tuple((int(i / ratio) for i in im.size))
        ratio_loc = tuple((int(i / ratio / 2) for i in im.size))
        im_center.paste(im.resize(ratio_size), ratio_loc)
        return im_center

    def im_joint(self, im_1, im_2, flag='x', color='white'):
        # 拼接
        size_1, size_2 = im_1.size, im_2.size
        if flag == 'x':
            joint = Image.new(
                im_1.mode, (size_1[0] + size_2[0], max(size_1[1], size_2[1])),
                color=color)
            loc_1 = (0, 0)
            loc_2 = (size_1[0], (size_1[1] - size_2[1]) // 2)
        else:
            joint = Image.new(
                im_1.mode, (max(size_1[0], size_2[0]), size_1[1] + size_2[1]),
                color=color)
            loc_1 = (0, 0)
            loc_2 = ((size_1[0] - size_2[0]) // 2, size_1[1])

        joint.paste(im_1, loc_1)
        joint.paste(im_2, loc_2)
        return joint

    def adjust_gname_im_dict(self, gname_im_dict):
        res_dict = {}
        for k, v in gname_im_dict.items():
            if v is not None:
                im = v.getchannel(1)
                im = self.im_bw_transpose(im)
                im = self.im_put_to_center(im)
                im = im.resize((im.size[0] // 2, im.size[1] // 2),
                               Image.ANTIALIAS)
                res_dict[k] = im
        return res_dict

    def joint_im_for_baidu(self, gname_im_dict):
        filename = tempfile.TemporaryFile(delete=False, suffix='.png')
        im = reduce(lambda left, right: self.im_joint(left, right),
                    [im for im in gname_im_dict.values()])
        im.save(filename.name)
        return filename

    def generate_fonts_dict(self, fonts):
        # 识别文字
        with tqdm(total=len(fonts), desc='recognize texts from baidu') as pbar:
            fonts_dict = {}
            for k, font in fonts.items():
                pbar.update(1)
                gname_im_dict = self.get_font_gname_im_dict(font)
                gname_im_dict = self.adjust_gname_im_dict(gname_im_dict)
                im_filename = self.joint_im_for_baidu(gname_im_dict)
                baidu_result = self.baidu_orc.general_basic(im_filename.name)
                im_filename.close()
                os.remove(im_filename.name)
                if baidu_result is not None:
                    gname_from_baidu = ''.join(
                        [w['words'] for w in baidu_result['words_result']])
                    if len(gname_im_dict) == len(gname_from_baidu):
                        gname_im_dict_recognized = {
                            k: v
                            for k, v in zip(gname_im_dict, gname_from_baidu)
                        }
                    fonts_dict[k] = gname_im_dict_recognized
        return fonts_dict


class Autohome_Font:
    def __init__(self, backend='matplotlib') -> None:
        # backend = 'matplotlib' or 'freetypepen'
        self.backend = backend
        self.backend_matplotlib = Autohome_Font_Matplotlib()
        self.backend_freetypepen = Autohome_Font_Freetypepen()

    def read_font_from_biz_content(self, biz_content):
        # 读取font到字典, 键与biz_content['biz_ttfs]一致
        fonts = {}
        for k, v in biz_content['biz_ttfs'].items():
            bio = BytesIO()
            bio.write(v)
            fonts[k] = TTFont(bio)
        return fonts

    def generate_fonts_dict(self, fonts):
        # 识别文字

        print('backend is %s' % self.backend)
        fonts_dict = None
        if self.backend == 'matplotlib':
            fonts_dict = self.backend_matplotlib.generate_fonts_dict(fonts)

        elif self.backend == 'freetypepen':
            fonts_dict = self.backend_freetypepen.generate_fonts_dict(fonts)

        return fonts_dict

    def replace_df_biz_replies_with_font(self, reply_content, page,
                                         fonts_dict):
        font_dict = fonts_dict[page]
        regex = re.compile('|'.join(map(re.escape, font_dict)))
        reply_content = regex.sub(lambda m: font_dict[m.group(0)],
                                  reply_content)
        return reply_content

    def replace_biz_content_by_fonts_dict(self, biz_content, fonts_dict):
        # 反向替换
        font_dict = fonts_dict[1]
        regex = re.compile('|'.join(map(re.escape, font_dict)))

        # 替换biz_contents
        biz_contents = biz_content['biz_contents']
        biz_contents = [
            regex.sub(lambda m: font_dict[m.group(0)], content)
            for content in biz_contents
        ]
        biz_content['biz_contents'] = biz_contents

        # 替换df_biz_imgs
        df_biz_imgs = biz_content['df_biz_imgs']
        df_biz_imgs['img_text'] = df_biz_imgs['img_text'].apply(
            lambda content: regex.sub(lambda m: font_dict[m.group(0)], content
                                      ))
        biz_content['df_biz_imgs'] = df_biz_imgs

        # 替换df_biz_replies
        df_biz_replies = biz_content['df_biz_replies']
        df_biz_replies['reply_content'] = df_biz_replies[[
            'reply_content', 'page'
        ]].apply(lambda row: self.replace_df_biz_replies_with_font(
            row['reply_content'], row['page'], fonts_dict),
                 axis=1)
        biz_content['df_biz_replies'] = df_biz_replies

        return biz_content

    def replace_biz_content(self, biz_content):
        self.fonts = self.read_font_from_biz_content(biz_content)
        self.fonts_dict = self.generate_fonts_dict(self.fonts)
        biz_content = self.replace_biz_content_by_fonts_dict(
            biz_content, self.fonts_dict)

        print('finished...')
        print('  biz_content replaced by fonts_dict.')
        print(
            '  refer to fonts and fonts_dict by self.fonts and self.fonts_dict.'
        )
        print('-' * 40)
        return biz_content


# %%
if __name__ == '__main__':
    biz_content = pickle.load(open('./output/bbs/biz/102697565.pkl', 'rb'))
    print(biz_content.keys())
    print(biz_content['biz_ttfs'].keys())
    # self = Autohome_Font(backend='freetypepen')
    self = Autohome_Font(backend='matplotlib')
    biz_content = self.replace_biz_content(biz_content)

# %%
# %%
