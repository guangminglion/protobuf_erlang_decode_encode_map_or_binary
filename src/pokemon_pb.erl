%% Copyright (c) 2009
%% Nick Gerakines <nick@gerakines.net>
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(pokemon_pb).
-export([encode_pikachu/1, decode_pikachu/1, delimited_decode_pikachu/1,encode_bin_pikachu/1,decode_bin_pikachu/1,delimited_decode_bin_pikachu/1,pack_bin_filed_pikachu/3,packed_bin_kv_pikachu/1]).
-export([has_extension/2, extension_size/1, get_extension/2,
         set_extension/3]).
-export([decode_extensions/1]).
-export([encode/1, decode/2, delimited_decode/2,decode_bin/2,encode_bin/1,delimited_decode_bin/2,decode_map/2,delimited_decode_map/2,encode_map/2]).
-export([int_to_enum/2, enum_to_int/2,to_record_pikachu/1,to_record_map/2]).
-record(pikachu, {abc, def, '$extensions' = dict:new()}).
%% ENCODE
encode([]) ->
    [];   
encode(Records) when is_list(Records) ->
    delimited_encode(Records);
encode(Record) ->
    encode(element(1, Record), Record).

encode_bin([]) ->
    [];
encode_bin(Records) when is_list(Records) ->
    delimited_encode_bin(Records);
encode_bin(Record) ->
    encode_bin(element(1, Record), Record).




encode_pikachu(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_pikachu(Record) when is_record(Record, pikachu) ->
    encode(pikachu, Record).


encode(pikachu, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(pikachu, Record) ->
    [iolist(pikachu, Record)|encode_extensions(Record)].

encode_bin(pikachu, Records) when is_list(Records) ->
    delimited_encode_bin(Records);
encode_bin(pikachu, Record) ->
    [iolist_bin(pikachu, Record)|encode_extensions(Record)].

encode_bin_pikachu(Records) when is_list(Records) ->
    delimited_encode_bin(Records);
encode_bin_pikachu(Record) when is_record(Record, pikachu) ->
    encode_bin(pikachu, Record).


packed_bin_kv_pikachu(KeyValue) ->
            F = fun({Key,Value},Acc) ->
                   case pack_bin_filed_pikachu(pikachu,Key,Value) of
                        {0,undefined,_} ->
                                        Acc;
                        {Fnum,Tag,Type,Val} ->
                                    [{Fnum,Tag,Type,Val}|Acc]
                    end end,
            NList = lists:foldl(F,[],KeyValue),
            NC = lists:keysort(1, NList),
            List = lists:foldl(fun({Fnum,Tag,Type,Val},Acc) ->
                                   NL = pack_bin(Fnum,Tag,Val,Type,[]),
                                   [NL,Acc] 
                               end,[],NC),
            List.            
pack_bin_filed_pikachu(pikachu,field,Value) ->
                    {fnum,tag,type,Value};
pack_bin_filed_pikachu(pikachu,_,Value) ->
                    {0,undefined,undefined,Value}.

encode_extensions(#pikachu{'$extensions' = Extends}) ->
    [pack(Key, Optionalness, Data, Type, Accer) ||
        {Key, {Optionalness, Data, Type, Accer}} <- dict:to_list(Extends)];
encode_extensions(_) -> [].

encode_extensions_map(#{'$extensions' := Extends} = Map) ->
    [pack(Key, Optionalness, Data, Type, Accer) ||
        {Key, {Optionalness, Data, Type, Accer}} <- dict:to_list(Extends)];
encode_extensions_map(_) -> [].



delimited_encode(Records) ->
    lists:map(fun(Record) ->
        IoRec = encode(Record),
        Size = iolist_size(IoRec),
        [protobuffs:encode_varint(Size), IoRec]
    end, Records).
delimited_encode_bin(Records) ->
    lists:map(fun(Record) ->
        IoRec = encode_bin(Record),
        Size = iolist_size(IoRec),
         error_logger:info_msg("delimited_encode_bin IoRec ~p ~p ~n",[IoRec,Record]),
        [protobuffs:encode_varint(Size), IoRec]
    end, Records).

with_default_get(Key,Map) ->
        maps:get(Key,Map,undefined).

iolist(pikachu, Record) ->
    [pack(1, required, with_default(Record#pikachu.abc, none), string, [])].
iolist_map(pikachu, Record) ->
    [pack(1, required, with_default_get(abc,Record), string, [])].
iolist_bin(pikachu, Record) ->
    [pack_bin(1, required, with_default_bin(Record#pikachu.abc, none,string), string, [])].

with_default(Default, Default) -> undefined;
with_default(Val, _) -> Val.

with_default_bin(Default, Default,_) -> undefined;
with_default_bin(undefined,_,_) -> undefined;
with_default_bin(Val, _,_) ->   Val.


pack(_, optional, undefined, _, _) -> [];
pack(_, optional, "undefined", _, _) ->
 [];
pack(_, optional, <<"undefined">>, _, _) -> [];
pack(_, repeated, undefined, _, _) -> [];
pack(_, repeated, "undefined", _, _) -> [];
pack(_, repeated, <<"undefined">>, _, _) -> [];
pack(_, repeated_packed, undefined, _, _) -> [];
pack(_, repeated_packed, [], _, _) -> [];

pack(FNum, required, undefined, Type, _) ->
    exit({error, {required_field_is_undefined, FNum, Type}});

pack(_, repeated, [], _, Acc) ->
    lists:reverse(Acc);

pack(FNum, repeated, [Head|Tail], Type, Acc) ->
    pack(FNum, repeated, Tail, Type, [pack(FNum, optional, Head, Type, [])|Acc]);

pack(FNum, repeated_packed, Data, Type, _) ->
    protobuffs:encode_packed(FNum, Data, Type);
pack(FNum, _, Data,Type, _) when is_map(Data) ->
     protobuffs:encode(FNum,encode_map(Type, Data), bytes);    
pack(FNum, _, Data, _, _) when is_tuple(Data) ->
    [RecName|_] = tuple_to_list(Data),
    protobuffs:encode(FNum, encode(RecName, Data), bytes);
pack(FNum, _, Data, _, _) when is_binary(Data) ->
    protobuffs:encode(FNum, Data, bytes);
pack(FNum, _, Data, Type, _) when Type=:=bool;Type=:=int32;Type=:=uint32;
                  Type=:=int64;Type=:=uint64;Type=:=sint32;
                  Type=:=sint64;Type=:=fixed32;Type=:=sfixed32;
                  Type=:=fixed64;Type=:=sfixed64;Type=:=string;
                  Type=:=bytes;Type=:=float;Type=:=double ->
    protobuffs:encode(FNum, Data, Type);

pack(FNum, _, Data, Type, _) when is_atom(Data) ->
    protobuffs:encode(FNum, enum_to_int(Type,Data), enum).

%%pack_bin
pack_bin(_, optional, undefined, _, _) ->
 [];
pack_bin(_, optional, "undefined", _, _) ->
 [];
pack_bin(_, optional, <<"undefined">>, _, _) -> 
[];
pack_bin(_, optional, <<>>, _, _) -> 
[];
pack_bin(_, repeated, <<"undefined">>, _, _) -> [];
pack_bin(_, repeated, "undefined", _, _) -> [];
pack_bin(_, repeated, undefined, _, _) -> [];

pack_bin(_, repeated_packed, undefined, _, _) -> [];
pack_bin(_, repeated_packed, [], _, _) -> [];

pack_bin(FNum, required, undefined, Type, _) ->
    exit({error, {required_field_is_undefined, FNum, Type}});
pack_bin(_, repeated, [], _, Acc) ->
    lists:reverse(Acc);
pack_bin(FNum, repeated, [Head|Tail], Type, Acc) ->
    pack_bin(FNum, repeated, Tail, Type, [pack_bin(FNum, optional, Head, Type, [])|Acc]);
pack_bin(FNum, repeated_packed, Data, Type, _) ->
    protobuffs:encode_packed_bin(FNum, Data, Type);
pack_bin(FNum, _, Data, Type, _) when Type=:=bool;Type=:=int32;Type=:=uint32;
                  Type=:=int64;Type=:=uint64;Type=:=sint32;
                  Type=:=sint64;Type=:=fixed32;Type=:=sfixed32;
                  Type=:=fixed64;Type=:=sfixed64;Type=:=string;
                  Type=:=bytes;Type=:=float;Type=:=double ->
    protobuffs:encode_bin(FNum, Data, Type);
pack_bin(FNum, _, Data, _, _) when is_tuple(Data) ->
    [RecName|_] = tuple_to_list(Data),
    protobuffs:encode_bin(FNum, encode_bin(RecName, Data), bytes);
pack_bin(FNum, _, Data, _, _) when is_binary(Data) ->
    protobuffs:encode_bin(FNum, Data, bytes);
pack_bin(FNum, _, Data, Type, _) when is_atom(Data) ->
    protobuffs:encode_bin(FNum, enum_to_int(Type,Data), enum).



enum_to_int(pikachu,value) ->
    1.

int_to_enum(_,Val) ->
    Val.

    


%% DECODE
decode_pikachu(Bytes) when is_binary(Bytes) ->
    decode(pikachu, Bytes).

decode_bin_pikachu(Bytes) when is_binary(Bytes) ->
    decode_bin(pikachu, Bytes).
delimited_decode_pikachu(Bytes) ->
    delimited_decode(pikachu, Bytes).
delimited_decode_bin_pikachu(Bytes) ->
    delimited_decode_bin(pikachu, Bytes).

delimited_decode(Type, Bytes) when is_binary(Bytes) ->
    delimited_decode(Type, Bytes, []).
delimited_decode_map(Type, Bytes) when is_binary(Bytes) ->
    delimited_decode_map(Type, Bytes, []).
delimited_decode_bin(Type, Bytes) when is_binary(Bytes) ->
    delimited_decode_bin(Type, Bytes, []).


delimited_decode_bin(_Type, <<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
delimited_decode_bin(Type, Bytes, Acc) ->
    try protobuffs:decode_varint(Bytes) of
        {Size, Rest} when size(Rest) < Size ->
            {lists:reverse(Acc), Bytes};
        {Size, Rest} ->
            <<MessageBytes:Size/binary, Rest2/binary>> = Rest,
            Message = decode_bin(Type, MessageBytes),
            delimited_decode_bin(Type, Rest2, [Message | Acc])
    catch
        % most likely cause is there isn't a complete varint in the buffer.
        _What:_Why ->
            {lists:reverse(Acc), Bytes}
    end.

delimited_decode(_Type, <<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
delimited_decode(Type, Bytes, Acc) ->
    try protobuffs:decode_varint(Bytes) of
        {Size, Rest} when size(Rest) < Size ->
            {lists:reverse(Acc), Bytes};
        {Size, Rest} ->
            <<MessageBytes:Size/binary, Rest2/binary>> = Rest,
            Message = decode(Type, MessageBytes),
            delimited_decode(Type, Rest2, [Message | Acc])
    catch
        % most likely cause is there isn't a complete varint in the buffer.
        _What:_Why ->
            {lists:reverse(Acc), Bytes}
    end.
delimited_decode_map(_Type, <<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
delimited_decode_map(Type, Bytes, Acc) ->
    try protobuffs:decode_varint(Bytes) of
        {Size, Rest} when size(Rest) < Size ->
            {lists:reverse(Acc), Bytes};
        {Size, Rest} ->
            <<MessageBytes:Size/binary, Rest2/binary>> = Rest,
            Message = decode_map(Type, MessageBytes),
            delimited_decode_map(Type, Rest2, [Message | Acc])
    catch
        % most likely cause is there isn't a complete varint in the buffer.
        _What:_Why ->
            {lists:reverse(Acc), Bytes}
    end.

encode_map(RecordsName,Records) ->
        [iolist_map(RecordsName, Records)|encode_extensions_map(Records)].
decode_bin(pikachu, Bytes) when is_binary(Bytes) ->
    Types = [{1, abc, int32, []}, {2, def, double, []}],
    Defaults = [],
    Decoded = decode_bin(Bytes, Types, Defaults),
   %% error_logger:info_report([{decode_bin,Decoded}]),
    to_record(pikachu, Decoded).

decode_bin(<<>>, Types, Acc) -> reverse_repeated_fields(Acc, Types);
decode_bin(Bytes, Types, Acc) ->
    {ok, FNum} = protobuffs:next_field_num(Bytes),
 %%   error_logger:info_report([{decode_bin,"***************",FNum,"**************",Types,Bytes}]),
    case lists:keyfind(FNum, 1, Types) of
        {FNum, Name, Type, Opts} ->
            {Value1, Rest1} =
                case lists:member(is_record, Opts) of
                    true ->
                        {{FNum, V}, R} = protobuffs:decode_bin(Bytes, bytes),
                        RecVal = decode_bin(Type, V),
                        {RecVal, R};
                    false ->
                        case lists:member(repeated_packed, Opts) of
                            true ->
                            {{FNum, V}, R} = protobuffs:decode_packed_bin(Bytes, Type),
                            {V, R};
                            false ->
                            {{FNum, V}, R} = protobuffs:decode_bin(Bytes, Type),
                            {V, R}
                        end
                end,
              %%  error_logger:info_report([{decode_bin,"***************",FNum,"**************",Value1,Rest1}]),  
            case lists:member(repeated, Opts) of
                true ->
                    case lists:keytake(FNum, 1, Acc) of
                        {value, {FNum, Name, List}, Acc1} ->
                            decode_bin(Rest1, Types, [{FNum, Name, [int_to_enum(Type,Value1)|List]}|Acc1]);
                        false ->
                            decode_bin(Rest1, Types, [{FNum, Name, [int_to_enum(Type,Value1)]}|Acc])
                    end;
                false ->
                    decode_bin(Rest1, Types, [{FNum, Name, int_to_enum(Type,Value1)}|Acc])
            end;
        false ->
            case lists:keyfind('$extensions', 2, Acc) of
                {_,_,Dict} ->
                    {{FNum, _V}, R} = protobuffs:decode_bin(Bytes, bytes),
                    Diff = size(Bytes) - size(R),
                    <<V:Diff/binary,_/binary>> = Bytes,
                    NewDict = dict:store(FNum, V, Dict),
                    NewAcc = lists:keyreplace('$extensions', 2, Acc, {false, '$extensions', NewDict}),
                    decode_bin(R, Types, NewAcc);
                _ ->
                    {ok, Skipped} = protobuffs:skip_next_field_bin(Bytes),
                    decode_bin(Skipped, Types, Acc)
            end
    end.

%%DECODE MAP

decode_map(pikachu, Bytes) when is_binary(Bytes) ->
    Types = [{1, abc, int32, []}, {2, def, double, []}],
    Defaults = [],
    Decoded = decode_map(Bytes, Types, Defaults),
    to_record_map(pikachu, Decoded).
decode(pikachu, Bytes) when is_binary(Bytes) ->
    Types = [{1, abc, int32, []}, {2, def, double, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(pikachu, Decoded).


decode_map(<<>>, Types, Acc) -> reverse_repeated_fields(Acc, Types);
decode_map(Bytes, Types, Acc) ->
    {ok, FNum} = protobuffs:next_field_num(Bytes),
    case lists:keyfind(FNum, 1, Types) of
        {FNum, Name, Type, Opts} ->
        %%error_logger:info_msg("decode pk ~p ~n",[{decode_pikachu,{FNum, Name, Type, Opts,Acc}}]),
            {Value1, Rest1} =
                case lists:member(is_record, Opts) of
                    true ->
                        {{FNum, V}, R} = protobuffs:decode(Bytes, bytes),
                        RecVal = decode_map(Type, V),
                        {RecVal, R};
                    false ->
                        case lists:member(repeated_packed, Opts) of
                            true ->
                            {{FNum, V}, R} = protobuffs:decode_packed(Bytes, Type),
                            {V, R};
                            false ->
                            {{FNum, V}, R} = protobuffs:decode(Bytes, Type),
                            {unpack_value(V, Type), R}
                        end
                end,
              %%    error_logger:info_report([{decode_bin,"***************",FNum,"**************",Value1,Rest1}]), 
            case lists:member(repeated, Opts) of
                true ->
                    case lists:keytake(FNum, 1, Acc) of
                        {value, {FNum, Name, List}, Acc1} ->
                            decode_map(Rest1, Types, [{FNum, Name, [int_to_enum(Type,Value1)|List]}|Acc1]);
                        false ->
                            decode_map(Rest1, Types, [{FNum, Name, [int_to_enum(Type,Value1)]}|Acc])
                    end;
                false ->
                    decode_map(Rest1, Types, [{FNum, Name, int_to_enum(Type,Value1)}|Acc])
            end;
        false ->
            case lists:keyfind('$extensions', 2, Acc) of
                {_,_,Dict} ->
                    {{FNum, _V}, R} = protobuffs:decode(Bytes, bytes),
                    Diff = size(Bytes) - size(R),
                    <<V:Diff/binary,_/binary>> = Bytes,
                    NewDict = dict:store(FNum, V, Dict),
                    NewAcc = lists:keyreplace('$extensions', 2, Acc, {false, '$extensions', NewDict}),
                    decode_map(R, Types, NewAcc);
                _ ->
                    {ok, Skipped} = protobuffs:skip_next_field(Bytes),
                    decode_map(Skipped, Types, Acc)
            end
    end.

decode(<<>>, Types, Acc) -> reverse_repeated_fields(Acc, Types);
decode(Bytes, Types, Acc) ->
    {ok, FNum} = protobuffs:next_field_num(Bytes),
    case lists:keyfind(FNum, 1, Types) of
        {FNum, Name, Type, Opts} ->
       %% error_logger:info_msg("decode pk ~p ~n",[{decode_pikachu,{FNum, Name, Type, Opts,Acc}}]),
            {Value1, Rest1} =
                case lists:member(is_record, Opts) of
                    true ->
                        {{FNum, V}, R} = protobuffs:decode(Bytes, bytes),
                        RecVal = decode(Type, V),
                        {RecVal, R};
                    false ->
                        case lists:member(repeated_packed, Opts) of
                            true ->
                            {{FNum, V}, R} = protobuffs:decode_packed(Bytes, Type),
                            {V, R};
                            false ->
                            {{FNum, V}, R} = protobuffs:decode(Bytes, Type),
                            {unpack_value(V, Type), R}
                        end
                end,
              %%    error_logger:info_report([{decode_bin,"***************",FNum,"**************",Value1,Rest1}]), 
            case lists:member(repeated, Opts) of
                true ->
                    case lists:keytake(FNum, 1, Acc) of
                        {value, {FNum, Name, List}, Acc1} ->
                            decode(Rest1, Types, [{FNum, Name, [int_to_enum(Type,Value1)|List]}|Acc1]);
                        false ->
                            decode(Rest1, Types, [{FNum, Name, [int_to_enum(Type,Value1)]}|Acc])
                    end;
                false ->
                    decode(Rest1, Types, [{FNum, Name, int_to_enum(Type,Value1)}|Acc])
            end;
        false ->
            case lists:keyfind('$extensions', 2, Acc) of
                {_,_,Dict} ->
                    {{FNum, _V}, R} = protobuffs:decode(Bytes, bytes),
                    Diff = size(Bytes) - size(R),
                    <<V:Diff/binary,_/binary>> = Bytes,
                    NewDict = dict:store(FNum, V, Dict),
                    NewAcc = lists:keyreplace('$extensions', 2, Acc, {false, '$extensions', NewDict}),
                    decode(R, Types, NewAcc);
                _ ->
                    {ok, Skipped} = protobuffs:skip_next_field(Bytes),
                    decode(Skipped, Types, Acc)
            end
    end.

reverse_repeated_fields(FieldList, Types) ->
    [ begin
          case lists:keyfind(FNum, 1, Types) of
              {FNum, Name, _Type, Opts} ->
                  case lists:member(repeated, Opts) of
                      true ->
                          {FNum, Name, lists:reverse(Value)};
                      _ ->
                          Field
                  end;
              _ -> Field
          end
      end || {FNum, Name, Value}=Field <- FieldList ].

unpack_value(Binary, string) when is_binary(Binary) ->
    binary_to_list(Binary);
unpack_value(Value, _) -> Value.

to_record(pikachu, DecodedTuples) ->
    %%error_logger:info_report([{pikachu,"**********",DecodedTuples}]),
    Record1 = lists:foldr(
        fun({_FNum, Name, Val}, Record) ->
            set_record_field(record_info(fields, pikachu), Record, Name, Val)
        end, #pikachu{}, DecodedTuples),
    decode_extensions(Record1).
to_record_map(pikachu, DecodedTuples) ->
    %%error_logger:info_report([{pikachu,"**********",DecodedTuples}]),
    Record1 = lists:foldr(
        fun({_FNum, Name, Val}, Record) ->
                 maps:put(Name,Val,Record)
        end, #{}, DecodedTuples),
    decode_extensions_map(Record1).

decode_extensions_map(#{'$extensions' := Extensions} = Record) ->
    Types = [],
    NewExtensions = decode_extensions_map(Types, dict:to_list(Extensions), []),
    Record#{'$extensions' => NewExtensions};
decode_extensions_map(Record) ->
    Record.
    
decode_extensions(#pikachu{'$extensions' = Extensions} = Record) ->
    Types = [],
    NewExtensions = decode_extensions(Types, dict:to_list(Extensions), []),
    Record#pikachu{'$extensions' = NewExtensions};
decode_extensions(Record) ->
    Record.


decode_extensions_map(_Types, [], Acc) ->
    maps:from_list(Acc);
decode_extensions_map(Types, [{Fnum, Bytes} | Tail], Acc) ->
    NewAcc = case lists:keyfind(Fnum, 1, Types) of
        {Fnum, Name, Type, Opts} ->
            {Value1, Rest1} =
                case lists:member(is_record, Opts) of
                    true ->
                        {{FNum, V}, R} = protobuffs:decode(Bytes, bytes),
                        RecVal = decode_map(Type, V),
                        {RecVal, R};
                    false ->
                        case lists:member(repeated_packed, Opts) of
                            true ->
                                {{FNum, V}, R} = protobuffs:decode_packed(Bytes, Type),
                                {V, R};
                            false ->
                                {{FNum, V}, R} = protobuffs:decode(Bytes, Type),
                                {unpack_value(V, Type), R}
                        end
                end,
            case lists:member(repeated, Opts) of
                true ->
                    case lists:keytake(FNum, 1, Acc) of
                        {value, {FNum, Name, List}, Acc1} ->
                            decode(Rest1, Types, [{FNum, Name, lists:reverse([int_to_enum(Type,Value1)|lists:reverse(List)])}|Acc1]);
                        false ->
                            decode(Rest1, Types, [{FNum, Name, [int_to_enum(Type,Value1)]}|Acc])
                    end;
                false ->
                    [{Fnum, {optional, int_to_enum(Type,Value1), Type, Opts}} | Acc]
            end;
        false ->
            [{Fnum, Bytes} | Acc]
    end,
    decode_extensions_map(Types, Tail, NewAcc).
%%
decode_extensions(_Types, [], Acc) ->
    dict:from_list(Acc);
decode_extensions(Types, [{Fnum, Bytes} | Tail], Acc) ->
    NewAcc = case lists:keyfind(Fnum, 1, Types) of
        {Fnum, Name, Type, Opts} ->
            {Value1, Rest1} =
                case lists:member(is_record, Opts) of
                    true ->
                        {{FNum, V}, R} = protobuffs:decode(Bytes, bytes),
                        RecVal = decode(Type, V),
                        {RecVal, R};
                    false ->
                        case lists:member(repeated_packed, Opts) of
                            true ->
                                {{FNum, V}, R} = protobuffs:decode_packed(Bytes, Type),
                                {V, R};
                            false ->
                                {{FNum, V}, R} = protobuffs:decode(Bytes, Type),
                                {unpack_value(V, Type), R}
                        end
                end,
            case lists:member(repeated, Opts) of
                true ->
                    case lists:keytake(FNum, 1, Acc) of
                        {value, {FNum, Name, List}, Acc1} ->
                            decode(Rest1, Types, [{FNum, Name, lists:reverse([int_to_enum(Type,Value1)|lists:reverse(List)])}|Acc1]);
                        false ->
                            decode(Rest1, Types, [{FNum, Name, [int_to_enum(Type,Value1)]}|Acc])
                    end;
                false ->
                    [{Fnum, {optional, int_to_enum(Type,Value1), Type, Opts}} | Acc]
            end;
        false ->
            [{Fnum, Bytes} | Acc]
    end,
    decode_extensions(Types, Tail, NewAcc).

set_record_field(Fields, Record, '$extensions', Value) ->
        Decodable = [],
    NewValue = decode_extensions(element(1, Record), Decodable, dict:to_list(Value)),
        Index = list_index('$extensions', Fields),
        erlang:setelement(Index+1,Record,NewValue);
set_record_field(Fields, Record, Field, Value) ->
    Index = list_index(Field, Fields),
    erlang:setelement(Index+1, Record, Value).

list_index(Target, List) ->  list_index(Target, List, 1).

list_index(Target, [Target|_], Index) -> Index;
list_index(Target, [_|Tail], Index) -> list_index(Target, Tail, Index+1);
list_index(_, [], _) -> -1.

extension_size(#pikachu{'$extensions' = Extensions}) ->
    dict:size(Extensions);
extension_size(_) ->
    0.

has_extension(#pikachu{'$extensions' = Extensions}, FieldKey) ->
    dict:is_key(FieldKey, Extensions);
has_extension(_Record, _FieldName) ->
    false.

get_extension(Record, fieldatom) when is_record(Record, pikachu) ->
    get_extension(Record, 1);
get_extension(#pikachu{'$extensions' = Extensions}, Int) when is_integer(Int) ->
    case dict:find(Int, Extensions) of
        {ok, {_Rule, Value, _Type, _Opts}} ->
            {ok, Value};
        {ok, Binary} ->
            {raw, Binary};
         error ->
             undefined
     end;
get_extension(_Record, _FieldName) ->
    undefined.

set_extension(#pikachu{'$extensions' = Extensions} = Record, fieldname, Value) ->
    NewExtends = dict:store(1, {rule, Value, type, []}, Extensions),
    {ok, Record#pikachu{'$extensions' = NewExtends}};
set_extension(Record, _, _) ->
    {error, Record}.

to_record_pikachu(pikachu) ->
        record_info(fields, pikachu);
to_record_pikachu(_) ->
        undefined.